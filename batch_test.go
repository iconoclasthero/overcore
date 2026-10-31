package main

import (
	"strings"
	"testing"
	"time"
)

func TestAlertIdentityMatchesPIDOrName(t *testing.T) {
	if !processIdentity(10, "worker").matches(processIdentity(10, "other")) {
		t.Fatal("same PID must match despite a changed name")
	}
	if !processIdentity(10, "worker").matches(processIdentity(20, "worker")) {
		t.Fatal("same process name must match despite a different PID")
	}
	if processIdentity(10, "worker").matches(processIdentity(20, "other")) {
		t.Fatal("different PIDs and names must not match")
	}
}

func TestBatchUsesFixedDeadlineAndPreservesMessages(t *testing.T) {
	var delivered []string
	n := &notifier{
		command: "/test",
		host:    "host",
		log:     discardLogger(),
		send: func(_, line string) error {
			delivered = append(delivered, line)
			return nil
		},
	}
	id := processIdentity(42, "worker")
	start := at(0)
	n.collect("first", []alertIdentity{id}, start)
	n.collect("second", []alertIdentity{id}, start.Add(4*time.Minute))

	if len(delivered) != 0 || len(n.batches) != 1 {
		t.Fatalf("before deadline: delivered=%d batches=%d", len(delivered), len(n.batches))
	}
	n.flushDue(start.Add(5 * time.Minute))
	if len(delivered) != 1 || !strings.Contains(delivered[0], "first") || !strings.Contains(delivered[0], "second") {
		t.Fatalf("expected one message containing both original alerts, got %q", delivered)
	}
	if len(n.batches) != 0 {
		t.Fatalf("batch not cleared: %d", len(n.batches))
	}
}

func TestBatchDoesNotExtendDeadlineAndStartsNewBatchAfterExpiry(t *testing.T) {
	var delivered []string
	n := &notifier{
		command: "/test",
		host:    "host",
		log:     discardLogger(),
		send: func(_, line string) error {
			delivered = append(delivered, line)
			return nil
		},
	}
	id := processIdentity(42, "worker")
	start := at(0)
	n.collect("first", []alertIdentity{id}, start)
	n.collect("second", []alertIdentity{id}, start.Add(4*time.Minute))
	n.collect("third", []alertIdentity{id}, start.Add(5*time.Minute))

	if len(delivered) != 1 || !strings.Contains(delivered[0], "first") || !strings.Contains(delivered[0], "second") || strings.Contains(delivered[0], "third") {
		t.Fatalf("old batch should flush before third alert: %q", delivered)
	}
	if len(n.batches) != 1 || len(n.batches[0].alerts) != 1 || !strings.Contains(n.batches[0].alerts[0].line, "third") {
		t.Fatalf("third alert should start a fresh batch: %+v", n.batches)
	}
}

func TestOverlappingIdentitiesMergeBatches(t *testing.T) {
	var delivered []string
	n := &notifier{
		command: "/test",
		host:    "host",
		log:     discardLogger(),
		send: func(_, line string) error {
			delivered = append(delivered, line)
			return nil
		},
	}
	a := processIdentity(1, "alpha")
	b := processIdentity(2, "beta")
	start := at(0)
	n.collect("alpha alert", []alertIdentity{a}, start)
	n.collect("beta alert", []alertIdentity{b}, start.Add(time.Minute))
	// A CPU alert naming both processes connects the two existing batches.
	n.collect("cpu alert", []alertIdentity{a, b}, start.Add(2*time.Minute))

	if len(n.batches) != 1 || len(n.batches[0].alerts) != 3 {
		t.Fatalf("overlapping batches were not merged: %+v", n.batches)
	}
	n.flushDue(start.Add(5 * time.Minute))
	if len(delivered) != 1 || !strings.Contains(delivered[0], "alpha alert") || !strings.Contains(delivered[0], "beta alert") || !strings.Contains(delivered[0], "cpu alert") {
		t.Fatalf("merged batch did not preserve all alerts: %q", delivered)
	}
}

func TestUnrelatedIdentitiesRemainSeparate(t *testing.T) {
	n := &notifier{command: "/test", host: "host", log: discardLogger()}
	n.collect("one", []alertIdentity{processIdentity(1, "one")}, at(0))
	n.collect("two", []alertIdentity{processIdentity(2, "two")}, at(1))
	if len(n.batches) != 2 {
		t.Fatalf("unrelated alerts were combined: %d batches", len(n.batches))
	}
}
