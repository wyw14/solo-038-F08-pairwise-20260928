package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/service"
)

func newTestRouter(t *testing.T) (*httptest.Server, *service.Runtime) {
	t.Helper()
	runtime, err := service.NewRuntime(service.Config{DataDir: t.TempDir(), FestoonCapacity: 120, RegisterGap: 50 * time.Millisecond, RegisterTolerance: 0.25})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewRouter(runtime))
	t.Cleanup(server.Close)
	return server, runtime
}

func postJSON(t *testing.T, url string, body string) (int, map[string]any) {
	t.Helper()
	response, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, decoded
}

func TestSpliceHandoverHTTPFlow(t *testing.T) {
	server, runtime := newTestRouter(t)

	if _, err := runtime.PrepareSplice("alice", 20); err != nil {
		t.Fatal(err)
	}

	status, pending := postJSON(t, server.URL+"/api/splice/handovers", `{"from":"alice","to":"bob"}`)
	if status != http.StatusAccepted || pending["status"] != "pending" {
		t.Fatalf("expected pending handover, got %d %v", status, pending)
	}
	if pending["from_lease_id"] == nil || pending["from_lease_id"] == "" {
		t.Fatal("pending handover must record the outgoing lease generation")
	}

	// A second request while one is pending is rejected.
	if status, _ := postJSON(t, server.URL+"/api/splice/handovers", `{"from":"alice","to":"carol"}`); status != http.StatusConflict {
		t.Fatalf("expected conflict for duplicate handover, got %d", status)
	}

	// Only bob can accept.
	if status, _ := postJSON(t, server.URL+"/api/splice/handovers/accept", `{"operator":"alice"}`); status != http.StatusConflict {
		t.Fatalf("expected conflict for wrong operator, got %d", status)
	}
	status, accepted := postJSON(t, server.URL+"/api/splice/handovers/accept", `{"operator":"bob"}`)
	if status != http.StatusOK || accepted["status"] != "accepted" {
		t.Fatalf("expected accepted handover, got %d %v", status, accepted)
	}
	if accepted["from_lease_id"] == accepted["to_lease_id"] {
		t.Fatal("accepted handover must move to a new lease generation")
	}

	response, err := http.Get(server.URL + "/api/splice/handovers")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var records []map[string]any
	if err := json.NewDecoder(response.Body).Decode(&records); err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0]["operation"] == nil || records[0]["status"] != "accepted" {
		t.Fatalf("expected one retained handover record, got %v", records)
	}
}

func TestSpliceHandoverHTTPRequiresPreparedSplice(t *testing.T) {
	server, _ := newTestRouter(t)

	if status, body := postJSON(t, server.URL+"/api/splice/handovers", `{"from":"alice","to":"bob"}`); status != http.StatusConflict {
		t.Fatalf("expected conflict without a prepared splice, got %d %v", status, body)
	}

	// Expire endpoint with no pending handover is an empty, successful sweep.
	status, body := postJSON(t, server.URL+"/api/splice/handovers/expire", `{}`)
	if status != http.StatusOK {
		t.Fatalf("expected 200 from expire sweep, got %d %v", status, body)
	}
}
