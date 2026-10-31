package main

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/procfs"
)

// tempHysteresis: a sensor must drop this many °C below the threshold before
// its "sustained" timer resets.
const tempHysteresis = 3.0

type config struct {
	procThreshold float64 // % of ONE core; may exceed 100 for multi-threaded processes
	coreThreshold float64 // % busy of a single logical CPU
	window        time.Duration
	interval      time.Duration
	hysteresis    float64 // percentage points below threshold before an alert clears
	renotify      time.Duration

	tempThreshold float64
	tempWindow    time.Duration
	tempMatch     *regexp.Regexp

	ignore         *regexp.Regexp // process comm names to skip
	discordDM      string
	notifyRecovery bool
}

// track is the per-entity (process / core / sensor) state.
type track struct {
	win   rateWindow
	alert alertState
	timer sustainTimer // temperature only
	proc  procSample   // process only: latest reading
}

type busyProc struct {
	s   procSample
	pct float64
}

type monitor struct {
	cfg config
	fs  procfs.FS
	log *slog.Logger
	out *notifier

	procs map[procKey]*track
	cores map[int64]*track
	temps map[string]*track

	top []busyProc // busiest processes over the latest interval

	loggedNoSensors bool
}

func newMonitor(cfg config, fs procfs.FS, log *slog.Logger, out *notifier) *monitor {
	return &monitor{
		cfg:   cfg,
		fs:    fs,
		log:   log,
		out:   out,
		procs: map[procKey]*track{},
		cores: map[int64]*track{},
		temps: map[string]*track{},
	}
}

// tick takes one sample of everything and evaluates all alert conditions.
func (m *monitor) tick(ctx context.Context) {
	m.out.flush()
	now := time.Now()
	m.checkProcs(now)
	m.checkCores(now) // after procs: core alerts name the busiest processes
	m.checkTemps(ctx, now)
}

// ---------------------------------------------------------------- processes

func (m *monitor) checkProcs(now time.Time) {
	samples, err := scanProcs(m.fs)
	if err != nil {
		m.log.Error("reading /proc failed", "err", err)
		return
	}

	seen := make(map[procKey]struct{}, len(samples))
	m.top = m.top[:0]

	for _, p := range samples {
		if m.cfg.ignore != nil && m.cfg.ignore.MatchString(p.comm) {
			continue
		}
		seen[p.key] = struct{}{}

		tr := m.procs[p.key]
		if tr == nil {
			tr = &track{}
			m.procs[p.key] = tr
		}
		tr.proc = p
		tr.win.add(p.t, p.cpuSecs, m.cfg.window)

		if r, ok := tr.win.recentPercent(); ok {
			m.top = append(m.top, busyProc{p, r})
		}

		avg, ok := tr.win.avgPercent(m.cfg.window)
		hot := ok && avg >= m.cfg.procThreshold
		clear := ok && avg < m.cfg.procThreshold-m.cfg.hysteresis

		switch tr.alert.step(hot, clear, now, m.cfg.renotify) {
		case actFire:
			recent, _ := tr.win.recentPercent()
			m.out.alertFor(fmt.Sprintf("⚠️  %s (pid %d) averaged %.1f%% of one core over %s (now %.1f%%, %d thread(s), last on cpu %d)%s",
				p.comm, p.pid, avg, fmtDur(m.cfg.window), recent, p.threads, p.lastCPU, m.cmdSuffix(p.pid)), processIdentity(p.pid, p.comm))
		case actRemind:
			m.out.alertFor(fmt.Sprintf("⚠️  STILL HOT: %s (pid %d) at %.1f%% of one core, flagged for %s%s",
				p.comm, p.pid, avg, fmtDur(tr.alert.age(now)), m.cmdSuffix(p.pid)), processIdentity(p.pid, p.comm))
		case actRecovered:
			m.recoveryFor(fmt.Sprintf("%s (pid %d) back to %.1f%% of one core after %s",
				p.comm, p.pid, avg, fmtDur(tr.alert.age(now))), processIdentity(p.pid, p.comm))
		}
	}

	for k, tr := range m.procs {
		if _, ok := seen[k]; ok {
			continue
		}
		if tr.alert.active {
			m.recoveryFor(fmt.Sprintf("%s (pid %d) exited after %s flagged",
				tr.proc.comm, tr.proc.pid, fmtDur(tr.alert.age(now))), processIdentity(tr.proc.pid, tr.proc.comm))
		}
		delete(m.procs, k)
	}

	sort.Slice(m.top, func(i, j int) bool { return m.top[i].pct > m.top[j].pct })
	if len(m.top) > 5 {
		m.top = m.top[:5]
	}
	if m.log.Enabled(context.Background(), slog.LevelDebug) {
		for _, b := range m.top {
			m.log.Debug("busy process", "pid", b.s.pid, "comm", b.s.comm, "recent_pct", fmt.Sprintf("%.1f", b.pct))
		}
	}
}

// -------------------------------------------------------------------- cores

func (m *monitor) checkCores(now time.Time) {
	samples, err := scanCPUs(m.fs)
	if err != nil {
		m.log.Error("reading /proc/stat failed", "err", err)
		return
	}

	seen := make(map[int64]struct{}, len(samples))
	for _, c := range samples {
		seen[c.id] = struct{}{}
		tr := m.cores[c.id]
		if tr == nil {
			tr = &track{}
			m.cores[c.id] = tr
		}
		tr.win.add(c.t, c.busy, m.cfg.window)

		avg, ok := tr.win.avgPercent(m.cfg.window)
		avg = min(avg, 100) // tick rounding can nudge a saturated core to 100.1
		if m.log.Enabled(context.Background(), slog.LevelDebug) {
			recent, _ := tr.win.recentPercent()
			m.log.Debug("core", "cpu", c.id, "recent_pct", fmt.Sprintf("%.1f", recent), "window_avg_pct", fmt.Sprintf("%.1f", avg), "window_full", ok)
		}
		hot := ok && avg >= m.cfg.coreThreshold
		clear := ok && avg < m.cfg.coreThreshold-m.cfg.hysteresis

		switch tr.alert.step(hot, clear, now, m.cfg.renotify) {
		case actFire:
			recent, _ := tr.win.recentPercent()
			m.out.alertFor(fmt.Sprintf("⚠️  cpu%d averaged %.1f%% busy over %s (now %.1f%%)%s",
				c.id, avg, fmtDur(m.cfg.window), recent, m.topSuffix()), m.coreIdentities(c.id)...)
		case actRemind:
			m.out.alertFor(fmt.Sprintf("⚠️  STILL HOT: cpu%d at %.1f%% busy, flagged for %s%s",
				c.id, avg, fmtDur(tr.alert.age(now)), m.topSuffix()), m.coreIdentities(c.id)...)
		case actRecovered:
			m.recoveryFor(fmt.Sprintf("cpu%d back to %.1f%% busy after %s", c.id, avg, fmtDur(tr.alert.age(now))), keyedIdentity("core", fmt.Sprint(c.id)))
		}
	}
	for id, tr := range m.cores {
		if _, ok := seen[id]; !ok { // CPU went offline
			if tr.alert.active {
				m.recoveryFor(fmt.Sprintf("cpu%d went offline while flagged", id), keyedIdentity("core", fmt.Sprint(id)))
			}
			delete(m.cores, id)
		}
	}
}

// -------------------------------------------------------------- temperature

func (m *monitor) checkTemps(ctx context.Context, now time.Time) {
	samples, err := readTemps(ctx, m.cfg.tempMatch)
	if err != nil {
		m.log.Error("reading temperatures failed", "err", err)
		return
	}
	if len(samples) == 0 && !m.loggedNoSensors {
		m.loggedNoSensors = true
		m.log.Info("no matching temperature sensors found; temperature alerts inactive", "match", m.cfg.tempMatch.String())
	}

	seen := make(map[string]struct{}, len(samples))
	for _, s := range samples {
		seen[s.key] = struct{}{}
		tr := m.temps[s.key]
		if tr == nil {
			tr = &track{}
			m.temps[s.key] = tr
		}
		elapsed := tr.timer.update(now, s.c, m.cfg.tempThreshold, m.cfg.tempThreshold-tempHysteresis)
		hot := elapsed >= m.cfg.tempWindow
		clear := !tr.timer.running()

		switch tr.alert.step(hot, clear, now, m.cfg.renotify) {
		case actFire:
			m.out.alertFor(fmt.Sprintf("🔥 %s at %.0f°C, at or above %.0f°C for %s",
				s.key, s.c, m.cfg.tempThreshold, fmtDur(elapsed)), keyedIdentity("sensor", s.key))
		case actRemind:
			m.out.alertFor(fmt.Sprintf("🔥 STILL HOT: %s at %.0f°C, over %.0f°C for %s",
				s.key, s.c, m.cfg.tempThreshold, fmtDur(elapsed)), keyedIdentity("sensor", s.key))
		case actRecovered:
			m.recoveryFor(fmt.Sprintf("%s cooled to %.0f°C", s.key, s.c), keyedIdentity("sensor", s.key))
		}
	}
	for k, tr := range m.temps {
		if _, ok := seen[k]; !ok {
			if tr.alert.active {
				m.recoveryFor(fmt.Sprintf("sensor %s disappeared while flagged", k), keyedIdentity("sensor", k))
			}
			delete(m.temps, k)
		}
	}
}

// ------------------------------------------------------------------ helpers

func (m *monitor) recovery(msg string) {
	m.recoveryFor(msg, nil...)
}

func (m *monitor) recoveryFor(msg string, identities ...alertIdentity) {
	if m.cfg.notifyRecovery {
		m.out.alertFor("✅ "+msg, identities...)
	} else {
		m.out.info("recovered: " + msg)
	}
}

func (m *monitor) coreIdentities(id int64) []alertIdentity {
	identities := []alertIdentity{keyedIdentity("core", fmt.Sprint(id))}
	for i, b := range m.top {
		if i == 3 || b.pct < 5 {
			break
		}
		identities = appendIdentity(identities, processIdentity(b.s.pid, b.s.comm))
	}
	return identities
}

func (m *monitor) cmdSuffix(pid int) string {
	if c := cmdline(m.fs, pid); c != "" {
		return ": " + c
	}
	return ""
}

// topSuffix names the busiest processes right now, so a core alert says who
// is responsible, not just that something is.
func (m *monitor) topSuffix() string {
	var parts []string
	for _, b := range m.top {
		if len(parts) == 3 || b.pct < 5 {
			break
		}
		parts = append(parts, fmt.Sprintf("%s(%d) %.0f%%", b.s.comm, b.s.pid, b.pct))
	}
	if len(parts) == 0 {
		return ""
	}
	return " — busiest now: " + strings.Join(parts, ", ")
}

// fmtDur renders durations tersely: 10m, 1h30m, 45s.
func fmtDur(d time.Duration) string {
	s := d.Round(time.Second).String()
	s = strings.Replace(s, "m0s", "m", 1)
	s = strings.Replace(s, "h0m", "h", 1)
	return s
}
