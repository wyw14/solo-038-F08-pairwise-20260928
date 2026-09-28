package splice

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/unwind"
)

func newHandoverCoordinator() *Coordinator {
	return NewCoordinator(NewState(), unwind.NewFestoon(120), NewLeaseManager())
}

func TestHandoverAcceptBlocksPreviousLease(t *testing.T) {
	now := time.Now()
	coordinator := newHandoverCoordinator()
	operation, lease, err := coordinator.Prepare("operator-a", 20, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.RequestHandover(lease, "operator-b", 30*time.Second, now); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.StartWorker(context.Background(), operation, lease, time.Hour); err != nil {
		t.Fatalf("current operator must keep control while the handover is pending: %v", err)
	}
	handover, relief, err := coordinator.AcceptHandover("operator-b", 10*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	if handover.Operation != operation || handover.Lease != lease.ID {
		t.Fatalf("handover record lost the splice operation or lease: %+v", handover)
	}
	if handover.FromFence != lease.Fence || handover.ToFence != relief.Fence || handover.ToFence <= handover.FromFence {
		t.Fatalf("handover record must keep both lease generations: %+v", handover)
	}
	if err := coordinator.Cancel(operation, lease, now); err == nil {
		t.Fatal("pre-handover instruction must not advance the splice")
	}
	if err := coordinator.Cancel(operation, relief, now); err != nil {
		t.Fatalf("new operator must be able to cancel after accepting: %v", err)
	}
	if _, ok := coordinator.CurrentLease(); ok {
		t.Fatal("cancelled splice must release the lease")
	}
}

func TestHandoverWithdrawKeepsOwnerResponsible(t *testing.T) {
	now := time.Now()
	coordinator := newHandoverCoordinator()
	operation, lease, err := coordinator.Prepare("operator-a", 20, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.RequestHandover(lease, "operator-b", 30*time.Second, now); err != nil {
		t.Fatal(err)
	}
	handover, err := coordinator.WithdrawHandover(lease, now)
	if err != nil {
		t.Fatal(err)
	}
	if handover.Status != HandoverWithdrawn || handover.Operation != operation {
		t.Fatalf("withdrawal record is incomplete: %+v", handover)
	}
	if _, _, err := coordinator.AcceptHandover("operator-b", 10*time.Second, now); err == nil {
		t.Fatal("withdrawn handover must not be acceptable")
	}
	if err := coordinator.StartWorker(context.Background(), operation, lease, time.Hour); err != nil {
		t.Fatalf("owner must stay responsible after withdrawal: %v", err)
	}
	if err := coordinator.Cancel(operation, lease, now); err != nil {
		t.Fatal(err)
	}
}

func TestHandoverExpiresWithoutConfirmation(t *testing.T) {
	now := time.Now()
	coordinator := newHandoverCoordinator()
	operation, lease, err := coordinator.Prepare("operator-a", 20, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.RequestHandover(lease, "operator-b", 5*time.Second, now); err != nil {
		t.Fatal(err)
	}
	later := now.Add(6 * time.Second)
	if _, _, err := coordinator.AcceptHandover("operator-b", 10*time.Second, later); err == nil {
		t.Fatal("expired handover must not be acceptable")
	}
	records := coordinator.Handovers(later)
	if len(records) != 1 || records[0].Status != HandoverExpired {
		t.Fatalf("timeout withdrawal must be recorded as expired: %+v", records)
	}
	if err := coordinator.StartWorker(context.Background(), operation, lease, time.Hour); err != nil {
		t.Fatalf("owner must stay responsible after the handover lapsed: %v", err)
	}
	if err := coordinator.Cancel(operation, lease, later); err != nil {
		t.Fatal(err)
	}
}

func TestHandoverRejectsForeignActors(t *testing.T) {
	now := time.Now()
	coordinator := newHandoverCoordinator()
	_, lease, err := coordinator.Prepare("operator-a", 20, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.RequestHandover(lease, "operator-b", 30*time.Second, now); err != nil {
		t.Fatal(err)
	}
	if _, _, err := coordinator.AcceptHandover("operator-c", 10*time.Second, now); err == nil {
		t.Fatal("handover must only be acceptable by the addressed operator")
	}
	intruder := Lease{ID: lease.ID, Owner: "operator-c", Fence: lease.Fence}
	if _, err := coordinator.WithdrawHandover(intruder, now); err == nil {
		t.Fatal("only the responsible operator may withdraw the handover")
	}
	if _, err := coordinator.RequestHandover(lease, "operator-a", 30*time.Second, now); err == nil {
		t.Fatal("handover target must differ from the current owner")
	}
	if _, err := coordinator.WithdrawHandover(lease, now); err != nil {
		t.Fatal(err)
	}
}
