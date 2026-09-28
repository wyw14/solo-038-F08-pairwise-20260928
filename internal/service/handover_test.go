package service

import (
	"context"
	"testing"
	"time"
)

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(Config{DataDir: t.TempDir(), FestoonCapacity: 120, RegisterGap: 50 * time.Millisecond, RegisterTolerance: 0.25})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestSpliceHandoverLifecycle(t *testing.T) {
	runtime := newTestRuntime(t)

	operation, err := runtime.spliceControl.Prepare("alice", 20, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	oldLease := runtime.spliceControl.CurrentLease()

	pending, err := runtime.RequestSpliceHandover("alice", "bob")
	if err != nil {
		t.Fatal(err)
	}
	if pending.Status != "pending" || pending.From != "alice" || pending.To != "bob" {
		t.Fatalf("unexpected handover: %+v", pending)
	}

	// Before acceptance alice is still responsible: her worker command is valid.
	if err := runtime.spliceControl.StartWorker(context.Background(), "alice", operation, time.Hour); err != nil {
		t.Fatalf("outgoing operator must remain in charge before acceptance: %v", err)
	}

	accepted, err := runtime.AcceptSpliceHandover("bob")
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Status != "accepted" || accepted.FromLeaseID != oldLease.ID || accepted.ToLeaseID == oldLease.ID {
		t.Fatalf("handover must record both lease generations: %+v", accepted)
	}

	// Alice's commands are fenced off now.
	if err := runtime.spliceControl.Cancel("alice", operation, time.Now()); err == nil {
		t.Fatal("old operator must not be able to advance the splice after handover")
	}
	if err := runtime.spliceControl.Cancel("bob", operation, time.Now()); err != nil {
		t.Fatalf("new operator must control the splice: %v", err)
	}

	records := runtime.SpliceHandovers()
	if len(records) != 1 || records[0].Operation != operation {
		t.Fatalf("handover record must retain the seam operation: %+v", records)
	}
}

func TestSpliceHandoverTimeoutRevocation(t *testing.T) {
	runtime := newTestRuntime(t)

	operation, err := runtime.spliceControl.Prepare("alice", 20, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.RequestSpliceHandover("alice", "bob"); err != nil {
		t.Fatal(err)
	}

	revoked := runtime.spliceControl.ExpireHandovers(time.Now().Add(time.Hour))
	if len(revoked) != 1 || revoked[0].Status != "revoked" {
		t.Fatalf("expected timeout revocation, got %+v", revoked)
	}

	// Alice retains control of the same seam and lease generation.
	if err := runtime.spliceControl.Cancel("alice", operation, time.Now()); err != nil {
		t.Fatalf("outgoing operator must resume after revocation: %v", err)
	}
}
