package splice

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

type HandoverStatus string

const (
	HandoverPending  HandoverStatus = "pending"
	HandoverAccepted HandoverStatus = "accepted"
	HandoverRevoked  HandoverStatus = "revoked"
)

// Handover records one shift-handover of a prepared splice seam. It keeps the
// seam operation together with the lease generations before and after the
// handover, so responsibility can be audited after the fact.
type Handover struct {
	ID          model.OperationID `json:"id"`
	Operation   model.OperationID `json:"operation"`
	Phase       Phase             `json:"phase"`
	From        string            `json:"from"`
	To          string            `json:"to"`
	Status      HandoverStatus    `json:"status"`
	FromLeaseID model.LeaseID     `json:"from_lease_id"`
	FromFence   uint64            `json:"from_fence"`
	ToLeaseID   model.LeaseID     `json:"to_lease_id"`
	ToFence     uint64            `json:"to_fence"`
	RequestedAt time.Time         `json:"requested_at"`
	ExpiresAt   time.Time         `json:"expires_at"`
	ResolvedAt  time.Time         `json:"resolved_at"`
	Reason      string            `json:"reason,omitempty"`
}

type HandoverLog struct {
	mu      sync.Mutex
	pending *Handover
	records []Handover
}

func NewHandoverLog() *HandoverLog {
	return &HandoverLog{}
}

// Open files a handover request. The outgoing operator stays responsible for
// the seam until the incoming operator accepts it or it times out.
func (log *HandoverLog) Open(from, to string, operation model.OperationID, phase Phase, fromLease Lease, ttl time.Duration, now time.Time) (Handover, error) {
	if from == "" || to == "" {
		return Handover{}, fmt.Errorf("splice handover operators are required")
	}
	if from == to {
		return Handover{}, fmt.Errorf("splice handover requires a different operator")
	}
	if !operation.Valid() || !fromLease.ID.Valid() {
		return Handover{}, fmt.Errorf("splice handover requires an active operation")
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.pending != nil {
		return Handover{}, fmt.Errorf("splice handover is already pending for operator %s", log.pending.To)
	}
	handover := Handover{ID: model.NewOperationID(), Operation: operation, Phase: phase, From: from, To: to, Status: HandoverPending, FromLeaseID: fromLease.ID, FromFence: fromLease.Fence, RequestedAt: now.UTC(), ExpiresAt: now.Add(ttl).UTC()}
	log.pending = &handover
	log.records = append(log.records, handover)
	return handover, nil
}

// PendingFor returns the open request only when it belongs to the accepting
// operator and has not expired.
func (log *HandoverLog) PendingFor(operator string, now time.Time) (Handover, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.pending == nil {
		return Handover{}, fmt.Errorf("splice handover is not pending")
	}
	if log.pending.To != operator {
		return Handover{}, fmt.Errorf("splice handover is held for operator %s", log.pending.To)
	}
	if !now.Before(log.pending.ExpiresAt) {
		return Handover{}, fmt.Errorf("splice handover has expired")
	}
	return *log.pending, nil
}

// Resolve closes the pending request after the lease and seam have switched.
func (log *HandoverLog) Resolve(id model.OperationID, status HandoverStatus, toLease Lease, now time.Time, reason string) (Handover, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.pending == nil || log.pending.ID != id {
		return Handover{}, fmt.Errorf("splice handover is not pending")
	}
	log.pending.Status = status
	log.pending.ToLeaseID = toLease.ID
	log.pending.ToFence = toLease.Fence
	log.pending.ResolvedAt = now.UTC()
	log.pending.Reason = reason
	resolved := *log.pending
	log.records[len(log.records)-1] = resolved
	log.pending = nil
	return resolved, nil
}

// Expire revokes any pending request whose acceptance window has elapsed.
func (log *HandoverLog) Expire(now time.Time) []Handover {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.pending == nil || now.Before(log.pending.ExpiresAt) {
		return nil
	}
	log.pending.Status = HandoverRevoked
	log.pending.ResolvedAt = now.UTC()
	log.pending.Reason = "acceptance timeout"
	revoked := *log.pending
	log.records[len(log.records)-1] = revoked
	log.pending = nil
	return []Handover{revoked}
}

// CancelOperation revokes a pending request because the seam itself moved
// (for example the outgoing operator cancelled the splice).
func (log *HandoverLog) CancelOperation(operation model.OperationID, now time.Time) *Handover {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.pending == nil || log.pending.Operation != operation {
		return nil
	}
	log.pending.Status = HandoverRevoked
	log.pending.ResolvedAt = now.UTC()
	log.pending.Reason = "splice operation advanced before acceptance"
	revoked := *log.pending
	log.records[len(log.records)-1] = revoked
	log.pending = nil
	return &revoked
}

func (log *HandoverLog) Pending() (Handover, bool) {
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.pending == nil {
		return Handover{}, false
	}
	return *log.pending, true
}

// Records returns all handover requests oldest first, including the pending one.
func (log *HandoverLog) Records() []Handover {
	log.mu.Lock()
	defer log.mu.Unlock()
	records := make([]Handover, len(log.records))
	copy(records, log.records)
	return records
}
