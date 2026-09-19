// overcore watches for the failure that percentage-of-total dashboards miss:
// a single process (or a single core) pegged for hours, days, or weeks.
//
// Thresholds are per *core*, not per machine. On a 16-core box one runaway
// thread is only 6% of total CPU and will never trip a normal alert, but it is
// 100% of a core. overcore alerts on that.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/prometheus/procfs"
)

const defaultFIFO = "/var/lib/discord/capnhook.fifo"

// Default sensors: CPU-ish hwmon/thermal names. Override with -temp-match.
const defaultTempMatch = `(?i)^(coretemp|k10temp|zenpower|x86_pkg_temp|cpu[-_]thermal|soc)`

func parseConfig(args []string) (cfg config, verbose bool, err error) {
	var tempMatch, ignore string

	fs := flag.NewFlagSet("overcore", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: overcore [flags]\n\n")
		fs.PrintDefaults()
	}
	fs.Float64Var(&cfg.procThreshold, "threshold", 95, "alert when a process averages at least this % of ONE core (>100 allowed for multi-threaded)")
	fs.Float64Var(&cfg.coreThreshold, "core-threshold", 95, "alert when a logical CPU averages at least this % busy")
	fs.DurationVar(&cfg.window, "duration", 10*time.Minute, "averaging window / how long usage must be sustained")
	fs.DurationVar(&cfg.interval, "interval", 10*time.Second, "sampling interval")
	fs.Float64Var(&cfg.hysteresis, "hysteresis", 5, "percentage points below threshold before an alert clears")
	fs.DurationVar(&cfg.renotify, "renotify", 6*time.Hour, "re-send a reminder this often while still hot (0 = alert once)")
	fs.Float64Var(&cfg.tempThreshold, "temp-threshold", 80, "temperature threshold, °C")
	fs.DurationVar(&cfg.tempWindow, "temp-duration", 5*time.Minute, "how long a sensor must stay at/above the temperature threshold")
	fs.StringVar(&tempMatch, "temp-match", defaultTempMatch, "regexp selecting temperature sensors by key, e.g. coretemp_core_0")
	fs.StringVar(&ignore, "ignore", "", "regexp of process names (comm) to ignore, e.g. '^(ffmpeg|x264)$'")
	fs.StringVar(&cfg.fifo, "fifo", defaultFIFO, "FIFO to send alerts to (empty to disable)")
	fs.BoolVar(&cfg.notifyRecovery, "notify-recovery", false, "also send recovery messages to the FIFO (always logged)")
	fs.BoolVar(&verbose, "v", false, "verbose: log per-core and top-process readings every interval")

	if err = fs.Parse(args); err != nil {
		return
	}

	switch {
	case cfg.interval <= 0:
		err = errors.New("-interval must be positive")
	case cfg.window < cfg.interval:
		err = errors.New("-duration must be at least one -interval")
	case cfg.tempWindow < 0:
		err = errors.New("-temp-duration must not be negative")
	case cfg.procThreshold <= 0 || cfg.coreThreshold <= 0:
		err = errors.New("thresholds must be positive")
	case cfg.hysteresis < 0:
		err = errors.New("-hysteresis must not be negative")
	}
	if err != nil {
		return
	}
	if cfg.tempMatch, err = regexp.Compile(tempMatch); err != nil {
		return cfg, verbose, fmt.Errorf("-temp-match: %w", err)
	}
	if ignore != "" {
		if cfg.ignore, err = regexp.Compile(ignore); err != nil {
			return cfg, verbose, fmt.Errorf("-ignore: %w", err)
		}
	}
	return cfg, verbose, nil
}

func main() {
	cfg, verbose, err := parseConfig(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "overcore:", err)
		os.Exit(2)
	}

	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, log *slog.Logger) error {
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		return fmt.Errorf("opening /proc: %w", err)
	}
	m := newMonitor(cfg, fs, log, newNotifier(log, cfg.fifo))

	log.Info("overcore starting",
		"proc_threshold_pct", cfg.procThreshold,
		"core_threshold_pct", cfg.coreThreshold,
		"window", cfg.window.String(),
		"interval", cfg.interval.String(),
		"renotify", cfg.renotify.String(),
		"temp_threshold_c", cfg.tempThreshold,
		"temp_window", cfg.tempWindow.String(),
		"fifo", cfg.fifo,
	)
	log.Info(fmt.Sprintf("first alerts possible after %s, once the averaging window has filled", fmtDur(cfg.window)))

	m.tick(ctx) // baseline sample
	t := time.NewTicker(cfg.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			return nil
		case <-t.C:
			m.tick(ctx)
		}
	}
}
