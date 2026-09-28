package service

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/splice"
)

// PrepareSplice reserves the festoon and opens the splice seam for an
// operator. The seam can then be advanced or handed to the next shift.
func (runtime *Runtime) PrepareSplice(owner string, meters float64) (model.OperationID, error) {
	if owner == "" {
		return "", fmt.Errorf("splice operator is required")
	}
	return runtime.spliceControl.Prepare(owner, meters, nil, time.Now())
}

// RequestSpliceHandover opens a shift handover for the prepared splice. The
// outgoing operator stays responsible until the successor accepts or the
// request times out and is revoked.
func (runtime *Runtime) RequestSpliceHandover(from, to string) (splice.Handover, error) {
	if from == "" || to == "" {
		return splice.Handover{}, fmt.Errorf("splice handover requires outgoing and incoming operators")
	}
	operation, ok := runtime.spliceControl.ActiveOperation()
	if !ok {
		return splice.Handover{}, fmt.Errorf("splice is not prepared for handover")
	}
	return runtime.spliceControl.RequestHandover(from, to, operation, time.Now())
}

func (runtime *Runtime) AcceptSpliceHandover(operator string) (splice.Handover, error) {
	if operator == "" {
		return splice.Handover{}, fmt.Errorf("splice handover operator is required")
	}
	return runtime.spliceControl.AcceptHandover(operator, time.Now())
}

func (runtime *Runtime) ExpireSpliceHandovers() []splice.Handover {
	return runtime.spliceControl.ExpireHandovers(time.Now())
}

func (runtime *Runtime) SpliceHandovers() []splice.Handover {
	return runtime.spliceControl.Handovers()
}
