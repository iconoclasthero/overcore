package main

import (
  "context"
  "errors"
  "fmt"
  "log/slog"
  "os"
  "os/exec"
  "time"
)

const (
  // Keep alert messages reasonably bounded. Discord's message limit is 2000
  // characters; leave some room for the timestamp/host prefix.
  maxMessageLine = 1900

  maxPending = 32

  // A broken network connection or wedged curl must not block the monitor.
  discordTimeout = 30 * time.Second

  defaultDiscordDM = "/usr/local/bin/discorddm"
)

type notifier struct {
  log     *slog.Logger
  host    string
  command string

  pending []string
	batches []*alertBatch

  // send is a replaceable test hook. Production uses discorddm.
  send func(command, line string) error

  onAlert func(msg string) // test hook
}

func newNotifier(log *slog.Logger, command string) *notifier {
  host, _ := os.Hostname()

  if command == "" {
    command = defaultDiscordDM
  }

  return &notifier{
    log:     log,
    host:    host,
    command: command,
    send:    sendDiscordDM,
  }
}

// alert logs the message and pushes it to Discord.
func (n *notifier) alert(msg string) {
  n.alertFor(msg, nil...)
}

func (n *notifier) alertFor(msg string, identities ...alertIdentity) {
  n.log.Warn(msg)
  if n.onAlert != nil {
    n.onAlert(msg)
  }
  n.collect(msg, identities, time.Now())
}

// info logs only.
func (n *notifier) info(msg string) {
  n.log.Info(msg)
}

func (n *notifier) push(msg string) {
  if n.command == "" {
    return
  }

  line := fmt.Sprintf(
    "[%s] %s: %s",
    time.Now().Format(time.RFC3339),
    n.host,
    oneLine(msg, maxMessageLine),
  )

  if len(n.pending) > 0 {
    // Keep ordering: if anything is already queued, queue behind it.
    n.enqueue(line)
    n.flush()
    return
  }

  if err := n.send(n.command, line); err != nil {
    n.log.Error("discorddm delivery failed", "command", n.command, "err", err)
    n.enqueue(line)
    n.flush()
  }
}

func (n *notifier) enqueue(line string) {
  if len(n.pending) >= maxPending {
    n.pending = n.pending[1:]
  }
  n.pending = append(n.pending, line)
}

// flush retries queued lines in order, stopping at the first failure.
// Called on every push and once per monitoring cycle, so an alert raised
// while Discord is unavailable is delivered once it becomes available
// instead of being silently lost.
func (n *notifier) flushPending() {
  if n.command == "" {
    return
  }

  for len(n.pending) > 0 {
    if err := n.send(n.command, n.pending[0]); err != nil {
      n.log.Debug(
        "discorddm not deliverable yet",
        "command", n.command,
        "queued", len(n.pending),
        "err", err,
      )
      return
    }

    n.pending = n.pending[1:]
  }
}

// sendDiscordDM invokes discorddm with the complete message as one argument.
//
// There is deliberately no shell involved here. The alert text is passed as
// an argv element, so quotes, dollar signs, backslashes, etc. are not subject
// to shell interpretation.
func sendDiscordDM(command, line string) error {
  ctx, cancel := context.WithTimeout(context.Background(), discordTimeout)
  defer cancel()

  cmd := exec.CommandContext(ctx, command, line)

  output, err := cmd.CombinedOutput()
  if ctx.Err() != nil {
    if errors.Is(ctx.Err(), context.DeadlineExceeded) {
      return fmt.Errorf("discorddm timed out after %s", discordTimeout)
    }
    return ctx.Err()
  }

  if err != nil {
    if len(output) > 0 {
      return fmt.Errorf("discorddm: %w: %s", err, oneLine(string(output), 500))
    }
    return fmt.Errorf("discorddm: %w", err)
  }

  return nil
}

// flush delivers expired alert batches, then retries failed Discord deliveries.
func (n *notifier) flush() {
	n.flushDue(time.Now())
	n.flushPending()
}
