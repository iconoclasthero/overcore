package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/procfs"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testMonitor(t *testing.T, mutate func(*config)) (*monitor, *[]string) {
	t.Helper()
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		t.Skip("no /proc:", err)
	}
	cfg := config{
		procThreshold: 90,
		coreThreshold: 101, // disable core alerts here; they'd be a duplicate signal
		window:        3 * time.Second,
		interval:      250 * time.Millisecond,
		hysteresis:    5,
		tempThreshold: 1000,
		tempWindow:    time.Hour,
		tempMatch:     regexp.MustCompile(`^$`),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	var alerts []string
	n := newNotifier(discardLogger(), "")
	n.onAlert = func(s string) { alerts = append(alerts, s) }
	return newMonitor(cfg, fs, discardLogger(), n), &alerts
}

func drive(m *monitor, d time.Duration) {
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		m.tick(context.Background())
		time.Sleep(m.cfg.interval)
	}
}

func startBusy(t *testing.T) *exec.Cmd {
	t.Helper()
	c := exec.Command("sh", "-c", "while :; do :; done")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Process.Kill(); _ = c.Wait() })
	return c
}

func startSleeper(t *testing.T) *exec.Cmd {
	t.Helper()
	c := exec.Command("sleep", "120")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Process.Kill(); _ = c.Wait() })
	return c
}

func TestDetectsPeggedProcessAndIgnoresIdleOne(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	busy, idle := startBusy(t), startSleeper(t)
	m, alerts := testMonitor(t, nil)

	drive(m, 6*time.Second)

	var hit bool
	for _, a := range *alerts {
		if strings.Contains(a, fmt.Sprintf("(pid %d)", idle.Process.Pid)) {
			t.Errorf("idle process alerted: %s", a)
		}
		if strings.Contains(a, fmt.Sprintf("(pid %d)", busy.Process.Pid)) {
			hit = true
			t.Log(a)
		}
	}
	if !hit {
		t.Fatalf("pegged process (pid %d) not detected; alerts=%q", busy.Process.Pid, *alerts)
	}
}

func TestNoAlertBeforeWindowFills(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	startBusy(t)
	m, alerts := testMonitor(t, nil)
	drive(m, 2*time.Second) // window is 3s
	if len(*alerts) != 0 {
		t.Fatalf("alerted on a partial window: %q", *alerts)
	}
}

func TestIgnoreRegexp(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	startBusy(t)
	m, alerts := testMonitor(t, func(c *config) { c.ignore = regexp.MustCompile(`^sh$`) })
	drive(m, 5*time.Second)
	if len(*alerts) != 0 {
		t.Fatalf("ignored process still alerted: %q", *alerts)
	}
}

func TestCoreAlertNamesCulprit(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	if n := len(mustCPUs(t)); n != 1 {
		t.Skipf("needs a 1-CPU machine so the busy loop saturates the only core; have %d", n)
	}
	busy := startBusy(t)
	m, alerts := testMonitor(t, func(c *config) {
		c.coreThreshold = 90
		c.procThreshold = 1000 // suppress process alert
	})
	drive(m, 6*time.Second)
	for _, a := range *alerts {
		if strings.Contains(a, "cpu0") && strings.Contains(a, fmt.Sprintf("(%d)", busy.Process.Pid)) {
			t.Log(a)
			return
		}
	}
	t.Fatalf("no core alert naming pid %d: %q", busy.Process.Pid, *alerts)
}

func mustCPUs(t *testing.T) []cpuSample {
	t.Helper()
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		t.Skip(err)
	}
	c, err := scanCPUs(fs)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestScanProcsSeesSelf(t *testing.T) {
	fs, err := procfs.NewFS("/proc")
	if err != nil {
		t.Skip(err)
	}
	ps, err := scanProcs(fs)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range ps {
		if p.pid == os.Getpid() {
			if p.key.start == 0 || p.comm == "" || p.threads < 1 {
				t.Fatalf("incomplete sample: %+v", p)
			}
			return
		}
	}
	t.Fatal("own pid not found")
}
