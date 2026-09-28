package splice

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

type Outcome struct {
	Operation model.OperationID `json:"operation"`
	Phase     Phase             `json:"phase"`
	At        time.Time         `json:"at"`
	Reason    string            `json:"reason"`
}

func Complete(state *State, operation model.OperationID, lease model.LeaseID, now time.Time) (Outcome, error) {
	phase, current, currentLease, _, _, _ := state.Snapshot()
	if current != operation || currentLease != lease || phase != PhaseBonding {
		return Outcome{}, fmt.Errorf("splice is not ready to complete")
	}
	if err := state.Advance(operation, lease, PhaseComplete, now); err != nil {
		return Outcome{}, err
	}
	return Outcome{Operation: operation, Phase: PhaseComplete, At: now.UTC()}, nil
}

func CancelOutcome(state *State, operation model.OperationID, lease model.LeaseID, reason string, now time.Time) (Outcome, error) {
	if reason == "" {
		return Outcome{}, fmt.Errorf("splice cancellation reason is required")
	}
	if err := state.Advance(operation, lease, PhaseCancelled, now); err != nil {
		return Outcome{}, err
	}
	return Outcome{Operation: operation, Phase: PhaseCancelled, At: now.UTC(), Reason: reason}, nil
}
