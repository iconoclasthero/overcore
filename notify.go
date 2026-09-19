package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"syscall"
	"time"
)

const (
	// Writes of at most PIPE_BUF (4096 on Linux) bytes to a FIFO are atomic,
	// so concurrent writers can't interleave with us.
	maxFIFOLine = 4000
	maxPending  = 32
)

type notifier struct {
	log  *slog.Logger
	host string
	fifo string // "" disables FIFO delivery

	pending  []string
	sendFIFO func(path, line string) error // replaceable in tests
	onAlert  func(msg string)              // test hook
}

func newNotifier(log *slog.Logger, fifo string) *notifier {
	host, _ := os.Hostname()
	return &notifier{log: log, host: host, fifo: fifo, sendFIFO: writeFIFO}
}

// alert logs the message and pushes it to the Discord FIFO.
func (n *notifier) alert(msg string) {
	n.log.Warn(msg)
	if n.onAlert != nil {
		n.onAlert(msg)
	}
	n.push(msg)
}

// info logs only.
func (n *notifier) info(msg string) { n.log.Info(msg) }

func (n *notifier) push(msg string) {
	if n.fifo == "" {
		return
	}
	line := fmt.Sprintf("[%s] %s: %s\n", time.Now().Format(time.RFC3339), n.host, oneLine(msg, maxFIFOLine-100))
	if len(n.pending) > 0 || n.sendFIFO(n.fifo, line) != nil {
		// Keep ordering: if anything is already queued, queue behind it.
		n.enqueue(line)
		n.flush()
	}
}

func (n *notifier) enqueue(line string) {
	if len(n.pending) >= maxPending {
		n.pending = n.pending[1:] // drop oldest
	}
	n.pending = append(n.pending, line)
}

// flush retries queued lines in order, stopping at the first failure. Called
// on every push and once per monitoring cycle, so an alert raised while the
// Discord bridge was down is delivered once its reader reappears instead of
// being silently lost.
func (n *notifier) flush() {
	if n.fifo == "" {
		return
	}
	for len(n.pending) > 0 {
		if err := n.sendFIFO(n.fifo, n.pending[0]); err != nil {
			n.log.Debug("fifo not deliverable yet", "fifo", n.fifo, "queued", len(n.pending), "err", err)
			return
		}
		n.pending = n.pending[1:]
	}
}

// writeFIFO opens the FIFO non-blocking and writes one line.
//
// A plain shell `>> fifo` (or os.OpenFile) blocks forever when nobody is
// reading, which would wedge the monitor exactly when something is wrong.
// With O_NONBLOCK, open(2) fails immediately with ENXIO when there is no
// reader, and write(2) fails with EAGAIN if the pipe is full.
func writeFIFO(path, line string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	if fi.Mode()&os.ModeNamedPipe == 0 {
		// Without O_APPEND a write to a regular file would clobber it.
		return fmt.Errorf("%s is not a FIFO", path)
	}
	fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return err // ENXIO: no reader
	}
	defer syscall.Close(fd)

	buf := []byte(line)
	for {
		_, err = syscall.Write(fd, buf)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}
