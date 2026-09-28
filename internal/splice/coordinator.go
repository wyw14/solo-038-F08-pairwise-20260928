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

const (
	leaseTTL       = 10 * time.Second
	handoverTTL    = 30 * time.Second
	resumeLeaseTTL = 10 * time.Second
)

type Coordinator struct {
	mu          sync.Mutex
	state       *State
	festoon     *unwind.Festoon
	leases      *LeaseManager
	handovers   *HandoverLog
	cancel      context.CancelFunc
	done        chan struct{}
	sweepCancel context.CancelFunc
	activity    atomic.Int64
	operation   model.OperationID
	lease       Lease
	reservation unwind.Reservation
}

func NewCoordinator(state *State, festoon *unwind.Festoon, leases *LeaseManager) *Coordinator {
	return &Coordinator{state: state, festoon: festoon, leases: leases, handovers: NewHandoverLog()}
}

func (coordinator *Coordinator) Prepare(owner string, meters float64, beforeCommit func(), now time.Time) (model.OperationID, error) {
	lease, err := coordinator.leases.Acquire(owner, leaseTTL, now)
	if err != nil {
		return "", err
	}
	operation := model.NewOperationID()
	if beforeCommit != nil {
		beforeCommit()
	}
	reservation, err := coordinator.festoon.Reserve(string(operation), meters)
	if err != nil {
		coordinator.leases.Release(lease)
		return "", err
	}
	if err := coordinator.state.Begin(operation, lease.ID, lease.Fence, now); err != nil {
		coordinator.festoon.Release(string(operation), reservation.Fence)
		coordinator.leases.Release(lease)
		return "", err
	}
	coordinator.mu.Lock()
	coordinator.operation = operation
	coordinator.lease = lease
	coordinator.reservation = reservation
	coordinator.mu.Unlock()
	return operation, nil
}

func (coordinator *Coordinator) StartWorker(parent context.Context, owner string, operation model.OperationID, interval time.Duration) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	lease, err := coordinator.authorize(owner, operation, time.Now())
	if err != nil {
		return err
	}
	phase, current, _, _, _, _ := coordinator.state.Snapshot()
	if current != operation || phase != PhasePrepared {
		return fmt.Errorf("splice worker cannot start for inactive operation")
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
	return coordinator.state.Advance(operation, lease.ID, PhaseFeeding, time.Now())
}

func (coordinator *Coordinator) Cancel(owner string, operation model.OperationID, now time.Time) error {
	coordinator.mu.Lock()
	lease, err := coordinator.authorize(owner, operation, now)
	if err != nil {
		coordinator.mu.Unlock()
		return err
	}
	cancel := coordinator.cancel
	done := coordinator.done
	if err := coordinator.state.Advance(operation, lease.ID, PhaseCancelled, now); err != nil {
		coordinator.mu.Unlock()
		return err
	}
	coordinator.handovers.CancelOperation(operation, now)
	coordinator.mu.Unlock()
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

// RequestHandover opens a shift handover for the prepared seam. The outgoing
// operator keeps responsibility until the successor accepts; the current
// lease is pinned for the acceptance window.
func (coordinator *Coordinator) RequestHandover(from, to string, operation model.OperationID, now time.Time) (Handover, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	lease, err := coordinator.authorize(from, operation, now)
	if err != nil {
		return Handover{}, err
	}
	phase, current, _, _, _, _ := coordinator.state.Snapshot()
	if current != operation || !phase.Active() {
		return Handover{}, fmt.Errorf("splice handover requires an active operation")
	}
	handover, err := coordinator.handovers.Open(from, to, operation, phase, lease, handoverTTL, now)
	if err != nil {
		return Handover{}, err
	}
	if !coordinator.leases.Pin(lease, handover.ExpiresAt) {
		return Handover{}, fmt.Errorf("splice lease cannot be pinned for handover")
	}
	coordinator.lease = coordinator.leases.Snapshot()
	return handover, nil
}

// AcceptHandover transfers the seam to the waiting operator. Afterwards the
// outgoing operator's lease generation is dead: its commands fail the fence
// and cannot advance the splice.
func (coordinator *Coordinator) AcceptHandover(operator string, now time.Time) (Handover, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	pending, err := coordinator.handovers.PendingFor(operator, now)
	if err != nil {
		return Handover{}, err
	}
	previous := Lease{ID: pending.FromLeaseID, Fence: pending.FromFence, Owner: pending.From}
	next, err := coordinator.leases.Transfer(previous, operator, leaseTTL, now)
	if err != nil {
		return Handover{}, err
	}
	if err := coordinator.state.ReplaceLease(pending.Operation, previous.ID, previous.Fence, next.ID, next.Fence, now); err != nil {
		return Handover{}, err
	}
	handover, err := coordinator.handovers.Resolve(pending.ID, HandoverAccepted, next, now, "")
	if err != nil {
		return Handover{}, err
	}
	coordinator.lease = next
	return handover, nil
}

// ExpireHandovers revokes acceptance windows that have elapsed and gives the
// outgoing operator back a fresh lease with the same generation, so the seam
// can continue without reopening the job.
func (coordinator *Coordinator) ExpireHandovers(now time.Time) []Handover {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	revoked := coordinator.handovers.Expire(now)
	if len(revoked) == 0 {
		return nil
	}
	previous := Lease{ID: revoked[0].FromLeaseID, Fence: revoked[0].FromFence, Owner: revoked[0].From}
	if coordinator.leases.Pin(previous, now.Add(resumeLeaseTTL)) {
		coordinator.lease = coordinator.leases.Snapshot()
	}
	return revoked
}

// StartSweeper revokes timed-out handovers automatically.
func (coordinator *Coordinator) StartSweeper(parent context.Context, interval time.Duration) {
	coordinator.mu.Lock()
	if coordinator.sweepCancel != nil {
		coordinator.sweepCancel()
	}
	ctx, cancel := context.WithCancel(parent)
	coordinator.sweepCancel = cancel
	coordinator.mu.Unlock()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				coordinator.ExpireHandovers(time.Now())
			}
		}
	}()
}

func (coordinator *Coordinator) authorize(owner string, operation model.OperationID, now time.Time) (Lease, error) {
	if owner == "" {
		return Lease{}, fmt.Errorf("splice operator is required")
	}
	if coordinator.operation != operation || coordinator.lease.ID == "" {
		return Lease{}, fmt.Errorf("splice operation fence rejected")
	}
	lease := coordinator.lease
	if lease.Owner != owner || !coordinator.leases.Validate(lease, now) {
		return Lease{}, fmt.Errorf("splice lease for operator %s is not valid", owner)
	}
	return lease, nil
}

func (coordinator *Coordinator) Handovers() []Handover {
	return coordinator.handovers.Records()
}

func (coordinator *Coordinator) ActiveOperation() (model.OperationID, bool) {
	phase, operation, _, _, _, _ := coordinator.state.Snapshot()
	return operation, phase.Active() && operation.Valid()
}

func (coordinator *Coordinator) CurrentLease() Lease {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.lease
}

func (coordinator *Coordinator) PendingHandover() (Handover, bool) {
	return coordinator.handovers.Pending()
}

func (coordinator *Coordinator) Activity() int64 {
	return coordinator.activity.Load()
}

func (coordinator *Coordinator) FestoonAvailable() float64 {
	return coordinator.festoon.Available()
}
