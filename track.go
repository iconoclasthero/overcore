package main

import "time"

// ---------------------------------------------------------------------------
// rateWindow: average rate of a monotonically increasing counter over a
// trailing window.
//
// The counter is cumulative CPU-seconds (per process or per core), so the
// "rate" is CPU-seconds per wall-second, i.e. a fraction of one core.
// 1.0 == one core fully busy, 2.5 == two and a half cores.
//
// Averaging over the *whole* window (rather than requiring every individual
// sample to be over the line) makes the detector immune to sampling jitter:
// a process that wobbles between 93% and 100% is a 97% process, not one that
// keeps resetting its own timer.
// ---------------------------------------------------------------------------

type sample struct {
	t time.Time
	v float64
}

type rateWindow struct {
	s []sample
}

// add records a new reading and discards history that is no longer needed.
// After trimming, s[0] is the newest sample that is at least `window` old
// (when one exists), so the span s[0]..last is always >= window once warm
// and never much larger.
func (w *rateWindow) add(t time.Time, v float64, window time.Duration) {
	if n := len(w.s); n > 0 && (v < w.s[n-1].v || !t.After(w.s[n-1].t)) {
		// Counter went backwards (CPU offlined/onlined, clock oddity):
		// the old history says nothing about the new counter. Start over.
		w.s = w.s[:0]
	}
	w.s = append(w.s, sample{t, v})

	cutoff := t.Add(-window)
	drop := 0
	for drop+1 < len(w.s) && !w.s[drop+1].t.After(cutoff) {
		drop++
	}
	if drop > 0 {
		w.s = append(w.s[:0], w.s[drop:]...)
	}
}

// avgPercent returns the average utilisation, in percent of one core, over
// the trailing window. ok is false until a full window of history exists, so
// a freshly-seen process can never trigger an alert on a partial window.
func (w *rateWindow) avgPercent(window time.Duration) (pct float64, ok bool) {
	if len(w.s) < 2 {
		return 0, false
	}
	first, last := w.s[0], w.s[len(w.s)-1]
	span := last.t.Sub(first.t)
	if span < window || span <= 0 {
		return 0, false
	}
	return (last.v - first.v) / span.Seconds() * 100, true
}

// recentPercent is the utilisation over just the most recent interval.
func (w *rateWindow) recentPercent() (pct float64, ok bool) {
	n := len(w.s)
	if n < 2 {
		return 0, false
	}
	a, b := w.s[n-2], w.s[n-1]
	dt := b.t.Sub(a.t).Seconds()
	if dt <= 0 {
		return 0, false
	}
	return (b.v - a.v) / dt * 100, true
}

// ---------------------------------------------------------------------------
// alertState: fire once, optionally remind, announce recovery.
// ---------------------------------------------------------------------------

type action int

const (
	actNone action = iota
	actFire
	actRemind
	actRecovered
)

type alertState struct {
	active   bool
	since    time.Time // when the alert first fired
	lastSent time.Time // last fire/remind
}

// step advances the state machine.
//
//	hot   – the condition for alerting currently holds
//	clear – the condition has definitively ended (kept separate from !hot so
//	        callers can apply hysteresis and avoid fire/recover/fire flapping)
//
// renotify > 0 re-sends a reminder that often while the condition persists;
// 0 means "tell me once".
func (a *alertState) step(hot, clear bool, now time.Time, renotify time.Duration) action {
	switch {
	case !a.active && hot:
		a.active, a.since, a.lastSent = true, now, now
		return actFire
	case a.active && clear:
		a.active = false
		return actRecovered
	case a.active && hot && renotify > 0 && now.Sub(a.lastSent) >= renotify:
		a.lastSent = now
		return actRemind
	}
	return actNone
}

// age is how long the alert has been active.
func (a *alertState) age(now time.Time) time.Duration { return now.Sub(a.since) }

// ---------------------------------------------------------------------------
// sustainTimer: "continuously above X for Y" for instantaneous readings such
// as temperature. Rises start the clock at `on`; the clock only resets when
// the reading falls below `off` (< on), giving a little hysteresis so a sensor
// hovering at the threshold doesn't restart the count every other sample.
// ---------------------------------------------------------------------------

type sustainTimer struct{ since time.Time }

func (s *sustainTimer) update(now time.Time, v, on, off float64) time.Duration {
	if v >= on {
		if s.since.IsZero() {
			s.since = now
		}
	} else if v < off {
		s.since = time.Time{}
	}
	if s.since.IsZero() {
		return 0
	}
	return now.Sub(s.since)
}

// running reports whether the reading is currently considered "hot".
func (s *sustainTimer) running() bool { return !s.since.IsZero() }
