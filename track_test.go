package main

import (
	"math"
//	"os"
//	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

// feed adds samples every `step` seconds for `n` steps, burning `load` cores.
func feed(w *rateWindow, start, n, step int, load float64, window time.Duration, base float64) float64 {
	v := base
	for i := 0; i <= n; i++ {
		w.add(at(start+i*step), v, window)
		v += load * float64(step)
	}
	return v
}

func near(a, b, eps float64) bool { return math.Abs(a-b) <= eps }

func TestWindowNotReadyUntilFull(t *testing.T) {
	const win = 600 * time.Second
	var w rateWindow
	// 59 steps of 10s = 590s < 600s window
	feed(&w, 0, 59, 10, 1.0, win, 0)
	if _, ok := w.avgPercent(win); ok {
		t.Fatal("must not report before a full window has elapsed")
	}
	w.add(at(600), 600, win)
	pct, ok := w.avgPercent(win)
	if !ok || !near(pct, 100, 0.01) {
		t.Fatalf("got %.2f ok=%v, want 100 true", pct, ok)
	}
}

func TestWindowSingleCorePegged(t *testing.T) {
	// The whole point of the tool: exactly one core, 98%.
	const win = 300 * time.Second
	var w rateWindow
	feed(&w, 0, 100, 10, 0.98, win, 12345)
	pct, ok := w.avgPercent(win)
	if !ok || !near(pct, 98, 0.01) {
		t.Fatalf("got %.2f ok=%v, want 98", pct, ok)
	}
}

func TestWindowMultiCoreExceeds100(t *testing.T) {
	const win = 60 * time.Second
	var w rateWindow
	feed(&w, 0, 20, 10, 2.5, win, 0)
	pct, _ := w.avgPercent(win)
	if !near(pct, 250, 0.01) {
		t.Fatalf("got %.2f want 250", pct)
	}
}

func TestWindowForgetsOldHistory(t *testing.T) {
	// A process that was pegged for ages and then went idle must fall out of
	// alert range, and a long-idle process that starts burning must rise —
	// which is exactly where `ps` lifetime-average %CPU fails.
	const win = 60 * time.Second
	var w rateWindow
	v := feed(&w, 0, 100, 10, 1.0, win, 0) // pegged until t=1000
	feed(&w, 1010, 20, 10, 0.0, win, v)    // idle for 200s more
	pct, ok := w.avgPercent(win)
	if !ok || pct > 1 {
		t.Fatalf("got %.2f ok=%v, want ~0 after going idle", pct, ok)
	}
}

func TestWindowBoundedMemory(t *testing.T) {
	const win = 60 * time.Second
	var w rateWindow
	feed(&w, 0, 10_000, 1, 1.0, win, 0)
	if len(w.s) > 65 {
		t.Fatalf("history not trimmed: %d samples for a 60s window at 1s interval", len(w.s))
	}
}

func TestWindowJitterTolerance(t *testing.T) {
	// Alternating 93%/100% averages 96.5%: over a 95 threshold. A "every
	// sample must exceed threshold" design would keep resetting here.
	const win = 200 * time.Second
	var w rateWindow
	v := 0.0
	for i := 0; i <= 40; i++ {
		w.add(at(i*10), v, win)
		if i%2 == 0 {
			v += 9.3
		} else {
			v += 10.0
		}
	}
	pct, ok := w.avgPercent(win)
	if !ok || pct < 95 || pct > 98 {
		t.Fatalf("got %.2f ok=%v", pct, ok)
	}
}

func TestWindowCounterResetRestartsHistory(t *testing.T) {
	const win = 30 * time.Second
	var w rateWindow
	feed(&w, 0, 10, 10, 1.0, win, 1000)
	w.add(at(200), 5, win) // counter went backwards
	if len(w.s) != 1 {
		t.Fatalf("history should restart, have %d samples", len(w.s))
	}
	if _, ok := w.avgPercent(win); ok {
		t.Fatal("no average possible from a single sample")
	}
}

func TestRecentPercent(t *testing.T) {
	const win = 600 * time.Second
	var w rateWindow
	w.add(at(0), 0, win)
	w.add(at(10), 5, win)
	pct, ok := w.recentPercent()
	if !ok || !near(pct, 50, 0.001) {
		t.Fatalf("got %.2f", pct)
	}
}

func TestAlertLifecycle(t *testing.T) {
	var a alertState
	re := time.Hour

	if got := a.step(false, false, at(0), re); got != actNone {
		t.Fatalf("idle: %v", got)
	}
	if got := a.step(true, false, at(10), re); got != actFire {
		t.Fatalf("expected fire, got %v", got)
	}
	if got := a.step(true, false, at(20), re); got != actNone {
		t.Fatalf("must not re-fire immediately, got %v", got)
	}
	if got := a.step(true, false, at(10+3600), re); got != actRemind {
		t.Fatalf("expected remind after renotify, got %v", got)
	}
	if got := a.step(true, false, at(10+3600+10), re); got != actNone {
		t.Fatalf("remind must reset its own clock, got %v", got)
	}
	// In the hysteresis band: neither hot nor clear -> stay quiet and stay active.
	if got := a.step(false, false, at(10+3700), re); got != actNone || !a.active {
		t.Fatalf("hysteresis band changed state: %v active=%v", got, a.active)
	}
	if got := a.step(false, true, at(10+3800), re); got != actRecovered {
		t.Fatalf("expected recovery, got %v", got)
	}
	if got := a.step(true, false, at(10+3900), re); got != actFire {
		t.Fatalf("must be able to fire again after recovery, got %v", got)
	}
}

func TestAlertRenotifyZeroMeansOnce(t *testing.T) {
	var a alertState
	a.step(true, false, at(0), 0)
	if got := a.step(true, false, at(999999), 0); got != actNone {
		t.Fatalf("renotify=0 must never remind, got %v", got)
	}
}

func TestSustainTimer(t *testing.T) {
	var s sustainTimer
	const on, off = 80.0, 77.0
	if d := s.update(at(0), 70, on, off); d != 0 || s.running() {
		t.Fatal("below threshold must not run")
	}
	if d := s.update(at(10), 81, on, off); d != 0 || !s.running() {
		t.Fatalf("crossing starts the clock at zero, got %v", d)
	}
	if d := s.update(at(70), 79, on, off); d != 60*time.Second { // dip inside the hysteresis band
		t.Fatalf("hysteresis band must not reset the clock, got %v", d)
	}
	if d := s.update(at(80), 76, on, off); d != 0 || s.running() {
		t.Fatalf("dropping below off must reset, got %v", d)
	}
}

func TestOneLine(t *testing.T) {
	got := oneLine("a\nb\x00c\td", 100)
	if got != "a bc d" {
		t.Fatalf("got %q", got)
	}
	if got := oneLine(strings.Repeat("é", 50), 10); len([]rune(got)) != 11 {
		t.Fatalf("truncation by runes failed: %q", got)
	}
}

func TestFmtDur(t *testing.T) {
	for in, want := range map[time.Duration]string{
		10 * time.Minute:               "10m",
		90 * time.Minute:               "1h30m",
		time.Hour:                      "1h",
		45 * time.Second:               "45s",
		10*time.Minute + 5*time.Second: "10m5s",
	} {
		if got := fmtDur(in); got != want {
			t.Errorf("fmtDur(%v)=%q want %q", in, got, want)
		}
	}
}

func TestNotifierQueuesWhileDiscordDown(t *testing.T) {
    n := &notifier{command: "/x", host: "h", log: discardLogger()}
    up := false
    var delivered []string

    n.send = func(_, line string) error {
        if !up {
            return syscall.ENXIO
        }
        delivered = append(delivered, line)
        return nil
    }

    n.push("one")
    n.push("two")

    if len(delivered) != 0 || len(n.pending) != 2 {
        t.Fatalf("delivered=%d pending=%d", len(delivered), len(n.pending))
    }

    up = true
    n.flush()

    if len(delivered) != 2 || len(n.pending) != 0 ||
        !strings.Contains(delivered[0], "one") ||
        !strings.Contains(delivered[1], "two") {
        t.Fatalf("expected in-order delivery, got %q pending=%d", delivered, len(n.pending))
    }
}
