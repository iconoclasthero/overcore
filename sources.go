package main

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/prometheus/procfs"
	"github.com/shirou/gopsutil/v4/sensors"
)

// procKey identifies a process for its whole lifetime. PID alone is not
// enough: PIDs get recycled, and a recycled PID must never inherit the CPU
// history of the process that used to own it. (The bash keyed on PID only.)
type procKey struct {
	pid   int
	start uint64 // starttime from /proc/PID/stat, in clock ticks since boot
}

type procSample struct {
	key     procKey
	pid     int
	comm    string
	cpuSecs float64 // cumulative user+system CPU seconds, all threads
	threads int
	lastCPU uint // CPU it last ran on (informational)
	t       time.Time
}

// scanProcs reads utime+stime for every process. Values come straight from
// /proc/PID/stat as integer ticks, and are cumulative for the whole thread
// group (including threads that have already exited).
func scanProcs(fs procfs.FS) ([]procSample, error) {
	procs, err := fs.AllProcs()
	if err != nil {
		return nil, err
	}
	out := make([]procSample, 0, len(procs))
	for _, p := range procs {
		st, err := p.Stat()
		if err != nil {
			continue // exited between listing and reading, or hidden by hidepid
		}
		out = append(out, procSample{
			key:     procKey{pid: p.PID, start: st.Starttime},
			pid:     p.PID,
			comm:    st.Comm,
			cpuSecs: st.CPUTime(),
			threads: st.NumThreads,
			lastCPU: st.Processor,
			t:       time.Now(),
		})
	}
	return out, nil
}

type cpuSample struct {
	id   int64
	busy float64 // cumulative busy seconds
	t    time.Time
}

// scanCPUs returns per-core cumulative busy time from /proc/stat.
//
// "Busy" = user+nice+system+irq+softirq.
//   - iowait is idle time (the CPU had nothing to run); the bash version
//     computed 100-%idle, which wrongly counted iowait as busy.
//   - steal is time the hypervisor gave to someone else; it isn't work this
//     guest did, so it isn't counted either.
//   - guest/guest_nice are already included in user/nice by the kernel and
//     must not be added again.
func scanCPUs(fs procfs.FS) ([]cpuSample, error) {
	st, err := fs.Stat()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	out := make([]cpuSample, 0, len(st.CPU))
	for id, c := range st.CPU {
		out = append(out, cpuSample{
			id:   id,
			busy: c.User + c.Nice + c.System + c.IRQ + c.SoftIRQ,
			t:    now,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out, nil
}

type tempSample struct {
	key string
	c   float64
}

// readTemps returns temperature sensors whose gopsutil key (hwmon name plus
// label, e.g. "coretemp_core_0", "k10temp_tctl") matches re.
func readTemps(ctx context.Context, re *regexp.Regexp) ([]tempSample, error) {
	stats, err := sensors.TemperaturesWithContext(ctx)
	if err != nil {
		// gopsutil returns partial results alongside a *Warnings error when
		// some sensor files were unreadable. That is not fatal.
		var w *sensors.Warnings
		if !errors.As(err, &w) {
			return nil, err
		}
	}
	seen := map[string]int{}
	out := make([]tempSample, 0, len(stats))
	for _, s := range stats {
		if re != nil && !re.MatchString(s.SensorKey) {
			continue
		}
		key := s.SensorKey
		// Two NVMe drives both report "nvme_composite"; keep keys unique so
		// their timers don't collide.
		if seen[key]++; seen[key] > 1 {
			key = fmt.Sprintf("%s#%d", key, seen[key])
		}
		out = append(out, tempSample{key: key, c: s.Temperature})
	}
	return out, nil
}

// cmdline returns a one-line rendering of /proc/PID/cmdline ("" for kernel
// threads or if the process is already gone).
func cmdline(fs procfs.FS, pid int) string {
	p, err := fs.Proc(pid)
	if err != nil {
		return ""
	}
	args, err := p.CmdLine()
	if err != nil || len(args) == 0 {
		return ""
	}
	return oneLine(strings.Join(args, " "), 200)
}

// oneLine strips control characters (so nothing in an argv can break the
// line-oriented FIFO protocol) and truncates to max runes.
func oneLine(s string, max int) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case unicode.IsControl(r):
			return -1
		}
		return r
	}, s)
	if r := []rune(s); len(r) > max {
		return string(r[:max]) + "…"
	}
	return s
}
