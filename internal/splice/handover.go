package splice

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

// HandoverStatus describes how an operator handover request resolved.
type HandoverStatus string

const (
	HandoverPending   HandoverStatus = "pending"
	HandoverAccepted  HandoverStatus = "accepted"
	HandoverWithdrawn HandoverStatus = "withdrawn"
	HandoverExpired   HandoverStatus = "expired"
)

// Handover records one attempt to move splice responsibility between
// operators. The record keeps the splice operation and both lease
// generations (fences) so the seam handoff stays auditable after the lease
// changes hands.
type Handover struct {
	ID          string            `json:"id"`
	Operation   model.OperationID `json:"operation"`
	Lease       model.LeaseID     `json:"lease"`
	FromOwner   string            `json:"from_owner"`
	ToOwner     string            `json:"to_owner"`
	FromFence   uint64            `json:"from_fence"`
	ToFence     uint64            `json:"to_fence"`
	Status      HandoverStatus    `json:"status"`
	RequestedAt time.Time         `json:"requested_at"`
	ExpiresAt   time.Time         `json:"expires_at"`
	ResolvedAt  time.Time         `json:"resolved_at"`
}

// RequestHandover opens a pending transfer of the active splice lease to
// toOwner. The current owner keeps responsibility until the request is
// accepted: the pending record does not touch the active lease.
func (manager *LeaseManager) RequestHandover(operation model.OperationID, lease Lease, toOwner string, ttl time.Duration, now time.Time) (Handover, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.lapseLocked(now)
	if !operation.Valid() {
		return Handover{}, fmt.Errorf("splice handover operation is required")
	}
	if toOwner == "" || ttl <= 0 {
		return Handover{}, fmt.Errorf("splice handover target and ttl are required")
	}
	if !manager.validLocked(lease, now) {
		return Handover{}, fmt.Errorf("splice handover requires the current lease")
	}
	if toOwner == manager.active.Owner {
		return Handover{}, fmt.Errorf("splice handover target already holds the lease")
	}
	if manager.pendingIndexLocked() >= 0 {
		return Handover{}, fmt.Errorf("splice handover is already pending")
	}
	handover := Handover{ID: string(model.NewOperationID()), Operation: operation, Lease: lease.ID, FromOwner: lease.Owner, ToOwner: toOwner, FromFence: lease.Fence, Status: HandoverPending, RequestedAt: now.UTC(), ExpiresAt: now.Add(ttl).UTC()}
	manager.handovers = append(manager.handovers, handover)
	return handover, nil
}

// AcceptHandover confirms a pending handover: the lease fence advances and
// toOwner becomes responsible. Instructions carrying the previous fence can
// no longer validate against the lease.
func (manager *LeaseManager) AcceptHandover(toOwner string, ttl time.Duration, now time.Time) (Handover, Lease, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.lapseLocked(now)
	index := manager.pendingIndexLocked()
	if index < 0 {
		return Handover{}, Lease{}, fmt.Errorf("there is no pending splice handover")
	}
	handover := manager.handovers[index]
	if toOwner == "" || handover.ToOwner != toOwner {
		return Handover{}, Lease{}, fmt.Errorf("splice handover is addressed to %s", handover.ToOwner)
	}
	if ttl <= 0 {
		return Handover{}, Lease{}, fmt.Errorf("splice handover lease ttl is required")
	}
	if manager.active.ID != handover.Lease || manager.active.Fence != handover.FromFence || manager.active.Owner != handover.FromOwner {
		return Handover{}, Lease{}, fmt.Errorf("splice lease changed since the handover was requested")
	}
	manager.fence++
	manager.active = Lease{ID: handover.Lease, Owner: toOwner, Fence: manager.fence, ExpiresAt: now.Add(ttl).UTC()}
	handover.Status = HandoverAccepted
	handover.ToFence = manager.fence
	handover.ResolvedAt = now.UTC()
	manager.handovers[index] = handover
	return handover, manager.active, nil
}

// WithdrawHandover lets the responsible operator retract a pending request
// before it is accepted.
func (manager *LeaseManager) WithdrawHandover(lease Lease, now time.Time) (Handover, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.lapseLocked(now)
	index := manager.pendingIndexLocked()
	if index < 0 {
		return Handover{}, fmt.Errorf("there is no pending splice handover")
	}
	handover := manager.handovers[index]
	if !manager.identityLocked(lease) || lease.Owner != handover.FromOwner {
		return Handover{}, fmt.Errorf("only the responsible operator can withdraw the splice handover")
	}
	handover.Status = HandoverWithdrawn
	handover.ResolvedAt = now.UTC()
	manager.handovers[index] = handover
	return handover, nil
}

// SweepHandovers expires pending requests whose confirmation window has
// closed and returns the records it lapsed.
func (manager *LeaseManager) SweepHandovers(now time.Time) []Handover {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.lapseLocked(now)
}

// Handovers returns every handover record, lapsing expired requests first.
func (manager *LeaseManager) Handovers(now time.Time) []Handover {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.lapseLocked(now)
	return append([]Handover(nil), manager.handovers...)
}

func (manager *LeaseManager) PendingHandover(now time.Time) (Handover, bool) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.lapseLocked(now)
	index := manager.pendingIndexLocked()
	if index < 0 {
		return Handover{}, false
	}
	return manager.handovers[index], true
}

func (manager *LeaseManager) lapseLocked(now time.Time) []Handover {
	lapsed := []Handover{}
	for index, record := range manager.handovers {
		if record.Status == HandoverPending && !now.Before(record.ExpiresAt) {
			record.Status = HandoverExpired
			record.ResolvedAt = now.UTC()
			manager.handovers[index] = record
			lapsed = append(lapsed, record)
		}
	}
	return lapsed
}

func (manager *LeaseManager) pendingIndexLocked() int {
	for index := len(manager.handovers) - 1; index >= 0; index-- {
		if manager.handovers[index].Status == HandoverPending {
			return index
		}
	}
	return -1
}
