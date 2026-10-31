package main

import (
	"fmt"
	"sort"
	"time"
)

// notificationWindow is fixed from the first alert in a batch. Later alerts
// never move the deadline.
const notificationWindow = 5 * time.Minute

type alertIdentity struct {
	pid  int
	comm string
	kind string
	key  string
}

type queuedAlert struct {
	at   time.Time
	line string
}

type alertBatch struct {
	deadline   time.Time
	identities []alertIdentity
	alerts     []queuedAlert
}

func processIdentity(pid int, comm string) alertIdentity {
	return alertIdentity{pid: pid, comm: comm}
}

func keyedIdentity(kind, key string) alertIdentity {
	return alertIdentity{kind: kind, key: key}
}

func (a alertIdentity) matches(b alertIdentity) bool {
	if a.pid > 0 && b.pid > 0 && a.pid == b.pid {
		return true
	}
	if a.comm != "" && b.comm != "" && a.comm == b.comm {
		return true
	}
	return a.kind != "" && a.kind == b.kind && a.key != "" && a.key == b.key
}

func identitiesMatch(a, b []alertIdentity) bool {
	for _, x := range a {
		for _, y := range b {
			if x.matches(y) {
				return true
			}
		}
	}
	return false
}

func appendIdentity(dst []alertIdentity, id alertIdentity) []alertIdentity {
	for _, existing := range dst {
		if existing == id {
			return dst
		}
	}
	return append(dst, id)
}

func (n *notifier) collect(msg string, identities []alertIdentity, now time.Time) {
	if n.command == "" {
		return
	}

	// Expire first: an alert arriving at or after a deadline belongs to a
	// new batch, never to the batch that has already reached its deadline.
	n.flushDue(now)

	prefix := fmt.Sprintf("[%s] %s: ", now.Format(time.RFC3339), n.host)
	line := prefix + oneLine(msg, maxMessageLine-len([]rune(prefix)))
	matches := make([]int, 0, len(n.batches))
	for i, batch := range n.batches {
		if identitiesMatch(batch.identities, identities) {
			matches = append(matches, i)
		}
	}

	var batch *alertBatch
	if len(matches) == 0 {
		batch = &alertBatch{deadline: now.Add(notificationWindow)}
		n.batches = append(n.batches, batch)
	} else {
		first := matches[0]
		batch = n.batches[first]
		for i := len(matches) - 1; i >= 1; i-- {
			idx := matches[i]
			other := n.batches[idx]
			if other.deadline.Before(batch.deadline) {
				batch.deadline = other.deadline
			}
			for _, id := range other.identities {
				batch.identities = appendIdentity(batch.identities, id)
			}
			batch.alerts = append(batch.alerts, other.alerts...)
			n.batches = append(n.batches[:idx], n.batches[idx+1:]...)
			if idx < first {
				first--
			}
		}
		batch = n.batches[first]
	}
	for _, id := range identities {
		batch.identities = appendIdentity(batch.identities, id)
	}
	batch.alerts = append(batch.alerts, queuedAlert{at: now, line: line})
	sort.SliceStable(batch.alerts, func(i, j int) bool {
		return batch.alerts[i].at.Before(batch.alerts[j].at)
	})
}

func (n *notifier) flushDue(now time.Time) {
	if n.command == "" {
		return
	}
	remaining := n.batches[:0]
	for _, batch := range n.batches {
		if now.Before(batch.deadline) {
			remaining = append(remaining, batch)
			continue
		}
		n.enqueueBatch(batch)
	}
	n.batches = remaining
	n.flushPending()
}

func (n *notifier) enqueueBatch(batch *alertBatch) {
	var chunk string
	for _, alert := range batch.alerts {
		if chunk != "" && len([]rune(chunk))+1+len([]rune(alert.line)) > 2000 {
			n.enqueue(chunk)
			chunk = ""
		}
		if chunk != "" {
			chunk += "\n"
		}
		chunk += alert.line
	}
	if chunk != "" {
		n.enqueue(chunk)
	}
}
