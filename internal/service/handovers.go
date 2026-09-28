package service

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/splice"
)

const (
	defaultSpliceHandoverTTL = 30 * time.Second
	maxSpliceHandoverTTL     = 5 * time.Minute
	spliceHandoverLeaseTTL   = 10 * time.Second
)

// SpliceHandoverView is the operator-facing snapshot of the splice lease and
// every handover record kept for the current seam.
type SpliceHandoverView struct {
	Lease   *splice.Lease     `json:"lease,omitempty"`
	Pending *splice.Handover  `json:"pending,omitempty"`
	Records []splice.Handover `json:"records"`
}

// RequestSpliceHandover opens a handover of the active splice lease to
// toOwner. The current operator remains responsible until the request is
// accepted.
func (runtime *Runtime) RequestSpliceHandover(toOwner string, ttl time.Duration) (splice.Handover, error) {
	if toOwner == "" {
		return splice.Handover{}, fmt.Errorf("handover target operator is required")
	}
	if ttl <= 0 {
		ttl = defaultSpliceHandoverTTL
	}
	if ttl > maxSpliceHandoverTTL {
		ttl = maxSpliceHandoverTTL
	}
	now := time.Now()
	job, ok := runtime.jobs.Current()
	if !ok {
		return splice.Handover{}, fmt.Errorf("there is no active job")
	}
	runtime.sweepSpliceHandovers(job, now)
	lease, ok := runtime.spliceControl.CurrentLease()
	if !ok {
		return splice.Handover{}, fmt.Errorf("there is no active splice lease to hand over")
	}
	handover, err := runtime.spliceControl.RequestHandover(lease, toOwner, ttl, now)
	if err != nil {
		return splice.Handover{}, err
	}
	runtime.persistSpliceHandover(job, handover, "splice_handover_requested", now)
	return handover, nil
}

// AcceptSpliceHandover confirms the pending handover for owner. The lease
// fence advances, so instructions issued before the confirmation can no
// longer advance the splice.
func (runtime *Runtime) AcceptSpliceHandover(owner string) (splice.Handover, error) {
	if owner == "" {
		return splice.Handover{}, fmt.Errorf("handover accepting operator is required")
	}
	now := time.Now()
	job, ok := runtime.jobs.Current()
	if !ok {
		return splice.Handover{}, fmt.Errorf("there is no active job")
	}
	runtime.sweepSpliceHandovers(job, now)
	handover, _, err := runtime.spliceControl.AcceptHandover(owner, spliceHandoverLeaseTTL, now)
	if err != nil {
		return splice.Handover{}, err
	}
	runtime.persistSpliceHandover(job, handover, "splice_handover_accepted", now)
	return handover, nil
}

// WithdrawSpliceHandover retracts the pending handover; the current operator
// keeps responsibility for the splice.
func (runtime *Runtime) WithdrawSpliceHandover() (splice.Handover, error) {
	now := time.Now()
	job, ok := runtime.jobs.Current()
	if !ok {
		return splice.Handover{}, fmt.Errorf("there is no active job")
	}
	runtime.sweepSpliceHandovers(job, now)
	lease, ok := runtime.spliceControl.CurrentLease()
	if !ok {
		return splice.Handover{}, fmt.Errorf("there is no active splice lease")
	}
	handover, err := runtime.spliceControl.WithdrawHandover(lease, now)
	if err != nil {
		return splice.Handover{}, err
	}
	runtime.persistSpliceHandover(job, handover, "splice_handover_withdrawn", now)
	return handover, nil
}

func (runtime *Runtime) SpliceHandovers() SpliceHandoverView {
	now := time.Now()
	if job, ok := runtime.jobs.Current(); ok {
		runtime.sweepSpliceHandovers(job, now)
	}
	view := SpliceHandoverView{Records: runtime.spliceControl.Handovers(now)}
	if view.Records == nil {
		view.Records = []splice.Handover{}
	}
	if lease, ok := runtime.spliceControl.CurrentLease(); ok {
		view.Lease = &lease
	}
	if pending, ok := runtime.spliceControl.PendingHandover(now); ok {
		view.Pending = &pending
	}
	return view
}

// sweepSpliceHandovers lapses handover requests whose confirmation window
// closed and journals each timeout withdrawal.
func (runtime *Runtime) sweepSpliceHandovers(job model.Job, now time.Time) {
	for _, lapsed := range runtime.spliceControl.SweepHandovers(now) {
		runtime.persistSpliceHandover(job, lapsed, "splice_handover_expired", now)
	}
}

// persistSpliceHandover journals the handover record with its splice
// operation and lease generations. Persistence failures are surfaced as
// incidents because the lease state has already moved.
func (runtime *Runtime) persistSpliceHandover(job model.Job, handover splice.Handover, kind string, now time.Time) {
	event, err := model.NewEvent(handover.Operation, job, kind, handover, now)
	if err != nil {
		runtime.RecordIncident("warning", "splice", fmt.Sprintf("handover event build failed: %v", err))
		return
	}
	if _, err := runtime.journal.Append(event); err != nil {
		runtime.RecordIncident("warning", "splice", fmt.Sprintf("handover journal append failed: %v", err))
	}
}
