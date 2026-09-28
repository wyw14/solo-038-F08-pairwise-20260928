package splice

import (
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/unwind"
)

func newTestCoordinator(t *testing.T) (*Coordinator, *State, *LeaseManager) {
	t.Helper()
	state := NewState()
	leases := NewLeaseManager()
	coordinator := NewCoordinator(state, unwind.NewFestoon(120), leases)
	return coordinator, state, leases
}

func TestHandoverKeepsOldOperatorResponsibleUntilAcceptance(t *testing.T) {
	coordinator, state, leases := newTestCoordinator(t)
	now := time.Now().UTC()

	operation, err := coordinator.Prepare("alice", 20, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	oldLease := leases.Snapshot()

	handover, err := coordinator.RequestHandover("alice", "bob", operation, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if handover.Status != HandoverPending {
		t.Fatalf("expected pending handover, got %s", handover.Status)
	}

	// Pinned lease keeps alice valid through the acceptance window.
	later := handover.ExpiresAt.Add(-time.Second)
	if !leases.Validate(oldLease, later) {
		t.Fatal("outgoing lease must remain valid while handover is pending")
	}
	phase, current, stateLease, leaseFence, _, _ := state.Snapshot()
	if current != operation || stateLease != oldLease.ID || leaseFence != oldLease.Fence || phase != PhasePrepared {
		t.Fatal("seam must still belong to alice's lease generation before acceptance")
	}

	// Only bob may accept.
	if _, err := coordinator.AcceptHandover("carol", later); err == nil {
		t.Fatal("unrelated operator must not accept the handover")
	}

	accepted, err := coordinator.AcceptHandover("bob", later)
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Status != HandoverAccepted || accepted.ToLeaseID == oldLease.ID || accepted.ToFence == oldLease.Fence {
		t.Fatal("accepted handover must record a new lease generation")
	}
	if accepted.Operation != operation || accepted.FromLeaseID != oldLease.ID || accepted.FromFence != oldLease.Fence {
		t.Fatal("handover record must retain the seam operation and old lease generation")
	}

	// Alice's lease generation is dead: validation and seam advancement fail.
	if leases.Validate(oldLease, later) {
		t.Fatal("old lease must be invalid after handover")
	}
	if err := state.Advance(operation, oldLease.ID, PhaseFeeding, later); err == nil {
		t.Fatal("old operator commands must not advance the splice after acceptance")
	}

	// Bob holds the new generation and can continue.
	newLease := leases.Snapshot()
	if newLease.Owner != "bob" {
		t.Fatalf("expected new owner bob, got %s", newLease.Owner)
	}
	if err := state.Advance(operation, newLease.ID, PhaseFeeding, later); err != nil {
		t.Fatalf("new operator must be able to advance the splice: %v", err)
	}

	records := coordinator.Handovers()
	if len(records) != 1 || records[0].Status != HandoverAccepted {
		t.Fatalf("expected one accepted handover record, got %+v", records)
	}
}

func TestHandoverTimeoutRevokesAndRestoresOutgoingLease(t *testing.T) {
	coordinator, _, leases := newTestCoordinator(t)
	now := time.Now().UTC()

	operation, err := coordinator.Prepare("alice", 20, nil, now)
	if err != nil {
		t.Fatal(err)
	}
	oldLease := leases.Snapshot()

	handover, err := coordinator.RequestHandover("alice", "bob", operation, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}

	// Acceptance past the deadline is rejected.
	deadline := handover.ExpiresAt.Add(time.Second)
	if _, err := coordinator.AcceptHandover("bob", deadline); err == nil {
		t.Fatal("expired handover must not be accepted")
	}

	revoked := coordinator.ExpireHandovers(deadline)
	if len(revoked) != 1 || revoked[0].Status != HandoverRevoked {
		t.Fatalf("expected one revoked handover, got %+v", revoked)
	}
	if revoked[0].Operation != operation || revoked[0].FromLeaseID != oldLease.ID || revoked[0].ToLeaseID.Valid() {
		t.Fatal("revoked record must retain the seam and old lease generation and have no successor")
	}

	// Alice keeps the same lease generation and can continue the seam.
	if !leases.Validate(oldLease, deadline) {
		t.Fatal("outgoing lease must be extended after timeout revocation")
	}
	if err := coordinator.Cancel("alice", operation, deadline); err != nil {
		t.Fatalf("outgoing operator must be able to continue after timeout: %v", err)
	}
}

func TestHandoverRequiresPreparedSpliceAndLeaseOwner(t *testing.T) {
	coordinator, _, _ := newTestCoordinator(t)
	now := time.Now().UTC()

	operation, err := coordinator.Prepare("alice", 20, nil, now)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := coordinator.RequestHandover("bob", "carol", operation, now); err == nil {
		t.Fatal("operator without the lease must not request a handover")
	}
	if _, err := coordinator.RequestHandover("alice", "alice", operation, now); err == nil {
		t.Fatal("handover to the same operator must be rejected")
	}
	if _, err := coordinator.RequestHandover("alice", "bob", model.NewOperationID(), now); err == nil {
		t.Fatal("handover for an unknown operation must be rejected")
	}
}
