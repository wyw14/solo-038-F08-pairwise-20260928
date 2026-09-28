package splice

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/unwind"
)

type Coordinator struct {
	mu          sync.Mutex
	state       *State
	festoon     *unwind.Festoon
	leases      *LeaseManager
	cancel      context.CancelFunc
	done        chan struct{}
	activity    atomic.Int64
	reservation unwind.Reservation
}

func NewCoordinator(state *State, festoon *unwind.Festoon, leases *LeaseManager) *Coordinator {
	return &Coordinator{state: state, festoon: festoon, leases: leases}
}

func (coordinator *Coordinator) Prepare(owner string, meters float64, beforeCommit func(), now time.Time) (model.OperationID, Lease, error) {
	lease, err := coordinator.leases.Acquire(owner, 10*time.Second, now)
	if err != nil {
		return "", Lease{}, err
	}
	operation := model.NewOperationID()
	if beforeCommit != nil {
		beforeCommit()
	}
	reservation, err := coordinator.festoon.Reserve(string(operation), meters)
	if err != nil {
		coordinator.leases.Release(lease)
		return "", Lease{}, err
	}
	if err := coordinator.state.Begin(operation, lease.ID, now); err != nil {
		coordinator.festoon.Release(string(operation), reservation.Fence)
		coordinator.leases.Release(lease)
		return "", Lease{}, err
	}
	coordinator.mu.Lock()
	coordinator.reservation = reservation
	coordinator.mu.Unlock()
	return operation, lease, nil
}

func (coordinator *Coordinator) StartWorker(parent context.Context, operation model.OperationID, lease Lease, interval time.Duration) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	phase, current, stateLease, _, _ := coordinator.state.Snapshot()
	if current != operation || phase != PhasePrepared {
		return fmt.Errorf("splice worker cannot start for inactive operation")
	}
	if err := coordinator.requireCurrentLease(stateLease, lease, time.Now()); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	coordinator.cancel = cancel
	coordinator.done = make(chan struct{})
	if err := coordinator.state.SetWorker(operation, string(model.NewOperationID())); err != nil {
		return err
	}
	go func(done chan struct{}) {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				coordinator.activity.Add(1)
			}
		}
	}(coordinator.done)
	return coordinator.state.Advance(operation, PhaseFeeding, time.Now())
}

// Cancel stops the splice worker and releases the lease. Cancellation stays
// available to the responsible owner even after the lease expires, but
// instructions issued before a handover are rejected.
func (coordinator *Coordinator) Cancel(operation model.OperationID, lease Lease, now time.Time) error {
	_, current, stateLease, _, _ := coordinator.state.Snapshot()
	if current != operation {
		return fmt.Errorf("splice operation fence rejected")
	}
	if err := coordinator.requireLeaseIdentity(stateLease, lease); err != nil {
		return err
	}
	coordinator.mu.Lock()
	cancel := coordinator.cancel
	done := coordinator.done
	coordinator.mu.Unlock()
	if err := coordinator.state.Advance(operation, PhaseCancelled, now); err != nil {
		return err
	}
	coordinator.leases.Release(lease)
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(time.Second):
			return fmt.Errorf("splice worker did not stop")
		}
	}
	return nil
}

// RequestHandover opens a handover of the active splice operation to
// toOwner. Until the new operator confirms, the current owner remains
// responsible and their instructions still advance the splice.
func (coordinator *Coordinator) RequestHandover(lease Lease, toOwner string, ttl time.Duration, now time.Time) (Handover, error) {
	operation, stateLease, err := coordinator.activeOperation()
	if err != nil {
		return Handover{}, err
	}
	if lease.ID != stateLease {
		return Handover{}, fmt.Errorf("splice lease does not match the active operation")
	}
	return coordinator.leases.RequestHandover(operation, lease, toOwner, ttl, now)
}

// AcceptHandover confirms the pending handover for toOwner. The lease fence
// advances, so instructions carrying the pre-handover fence can no longer
// advance the splice.
func (coordinator *Coordinator) AcceptHandover(toOwner string, ttl time.Duration, now time.Time) (Handover, Lease, error) {
	if _, _, err := coordinator.activeOperation(); err != nil {
		return Handover{}, Lease{}, err
	}
	return coordinator.leases.AcceptHandover(toOwner, ttl, now)
}

// WithdrawHandover retracts the pending handover; the current owner keeps
// responsibility for the splice.
func (coordinator *Coordinator) WithdrawHandover(lease Lease, now time.Time) (Handover, error) {
	_, stateLease, err := coordinator.activeOperation()
	if err != nil {
		return Handover{}, err
	}
	if lease.ID != stateLease {
		return Handover{}, fmt.Errorf("splice lease does not match the active operation")
	}
	return coordinator.leases.WithdrawHandover(lease, now)
}

func (coordinator *Coordinator) SweepHandovers(now time.Time) []Handover {
	return coordinator.leases.SweepHandovers(now)
}

func (coordinator *Coordinator) Handovers(now time.Time) []Handover {
	return coordinator.leases.Handovers(now)
}

func (coordinator *Coordinator) PendingHandover(now time.Time) (Handover, bool) {
	return coordinator.leases.PendingHandover(now)
}

func (coordinator *Coordinator) CurrentLease() (Lease, bool) {
	return coordinator.leases.Current()
}

func (coordinator *Coordinator) Activity() int64 {
	return coordinator.activity.Load()
}

func (coordinator *Coordinator) FestoonAvailable() float64 {
	return coordinator.festoon.Available()
}

func (coordinator *Coordinator) activeOperation() (model.OperationID, model.LeaseID, error) {
	phase, operation, lease, _, _ := coordinator.state.Snapshot()
	switch phase {
	case PhasePrepared, PhaseFeeding, PhaseBonding:
		return operation, lease, nil
	default:
		return "", "", fmt.Errorf("there is no active splice operation")
	}
}

func (coordinator *Coordinator) requireCurrentLease(stateLease model.LeaseID, lease Lease, now time.Time) error {
	if lease.ID != stateLease || !coordinator.leases.Validate(lease, now) {
		return fmt.Errorf("splice command carries a stale lease")
	}
	return nil
}

func (coordinator *Coordinator) requireLeaseIdentity(stateLease model.LeaseID, lease Lease) error {
	if lease.ID != stateLease || !coordinator.leases.Matches(lease) {
		return fmt.Errorf("splice command carries a stale lease")
	}
	return nil
}
