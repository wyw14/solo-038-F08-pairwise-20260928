package splice

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

type Phase string

const (
	PhaseIdle      Phase = "idle"
	PhasePrepared  Phase = "prepared"
	PhaseFeeding   Phase = "feeding"
	PhaseBonding   Phase = "bonding"
	PhaseCancelled Phase = "cancelled"
	PhaseComplete  Phase = "complete"
)

func (phase Phase) Active() bool {
	return phase == PhasePrepared || phase == PhaseFeeding || phase == PhaseBonding
}

type State struct {
	mu         sync.RWMutex
	phase      Phase
	operation  model.OperationID
	lease      model.LeaseID
	leaseFence uint64
	worker     string
	updatedAt  time.Time
}

func NewState() *State {
	return &State{phase: PhaseIdle}
}

func (state *State) Begin(operation model.OperationID, lease model.LeaseID, fence uint64, now time.Time) error {
	if !operation.Valid() || !lease.Valid() || fence == 0 {
		return fmt.Errorf("splice operation and lease are required")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.phase != PhaseIdle && state.phase != PhaseComplete && state.phase != PhaseCancelled {
		return fmt.Errorf("splice operation is already active")
	}
	state.operation = operation
	state.lease = lease
	state.leaseFence = fence
	state.phase = PhasePrepared
	state.updatedAt = now.UTC()
	return nil
}

func (state *State) Advance(operation model.OperationID, lease model.LeaseID, next Phase, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if operation != state.operation || lease != state.lease {
		return fmt.Errorf("splice operation fence rejected")
	}
	allowed := map[Phase]map[Phase]bool{PhasePrepared: {PhaseFeeding: true, PhaseCancelled: true}, PhaseFeeding: {PhaseBonding: true, PhaseCancelled: true}, PhaseBonding: {PhaseComplete: true, PhaseCancelled: true}}
	if !allowed[state.phase][next] {
		return fmt.Errorf("splice transition from %s to %s is invalid", state.phase, next)
	}
	state.phase = next
	state.updatedAt = now.UTC()
	return nil
}

// ReplaceLease hands the active operation to a new lease generation. The
// previous lease must match exactly, so a stale owner cannot move the seam.
func (state *State) ReplaceLease(operation model.OperationID, previous model.LeaseID, previousFence uint64, next model.LeaseID, nextFence uint64, now time.Time) error {
	if !next.Valid() || nextFence == 0 {
		return fmt.Errorf("splice handover lease is required")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if operation != state.operation || previous != state.lease || previousFence != state.leaseFence {
		return fmt.Errorf("splice handover fence rejected")
	}
	if state.phase != PhasePrepared && state.phase != PhaseFeeding && state.phase != PhaseBonding {
		return fmt.Errorf("splice handover requires an active operation")
	}
	state.lease = next
	state.leaseFence = nextFence
	state.updatedAt = now.UTC()
	return nil
}

func (state *State) SetWorker(operation model.OperationID, worker string) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if operation != state.operation || worker == "" {
		return fmt.Errorf("splice worker fence rejected")
	}
	state.worker = worker
	return nil
}

func (state *State) Snapshot() (Phase, model.OperationID, model.LeaseID, uint64, string, time.Time) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.phase, state.operation, state.lease, state.leaseFence, state.worker, state.updatedAt
}
