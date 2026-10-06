package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/domains18/kombucha/core"
	"github.com/domains18/kombucha/storage/engine"
	"github.com/domains18/kombucha/storage/wal"
)

func setupTestServer(t *testing.T, authToken string) (*Server, *engine.Engine, *ParkingLot) {
	dir := t.TempDir()
	parking := NewParkingLot()

	eng, err := engine.Open(engine.Config{
		DataDir: dir,
		WALOptions: wal.Options{
			Sync: true,
		},
		OnJobEligible: func(queue string) {
			parking.Signal(queue)
		},
	})
	if err != nil {
		t.Fatalf("Engine open failed: %v", err)
	}

	srv := NewServer(ServerConfig{
		Engine:     eng,
		ParkingLot: parking,
		AuthToken:  authToken,
	})

	return srv, eng, parking
}

func TestAPI_JobLifecycle(t *testing.T) {
	srv, eng, _ := setupTestServer(t, "secret-token")
	defer eng.Close()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	client := ts.Client()

	// 1. Submit Job
	submitBody := []byte(`{"type":"email.welcome","queue":"default","payload":{"user_id":42}}`)
	req, _ := http.NewRequest("POST", ts.URL+"/v1/jobs", bytes.NewReader(submitBody))
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
	}

	var submitResp SubmitResponse
	_ = json.NewDecoder(resp.Body).Decode(&submitResp)
	resp.Body.Close()

	if submitResp.ID == "" || submitResp.State != "pending" {
		t.Fatalf("unexpected submit response: %+v", submitResp)
	}

	// 2. Query Job
	req, _ = http.NewRequest("GET", ts.URL+"/v1/jobs/"+submitResp.ID, nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var job core.Job
	_ = json.NewDecoder(resp.Body).Decode(&job)
	resp.Body.Close()
	if job.Type != "email.welcome" {
		t.Fatalf("expected email.welcome, got %s", job.Type)
	}

	// 3. Lease Job
	leaseBody := []byte(`{"worker_id":"worker-1","queues":["default"],"max":1,"lease_seconds":30}`)
	req, _ = http.NewRequest("POST", ts.URL+"/v1/lease", bytes.NewReader(leaseBody))
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var leaseResp LeaseResponse
	_ = json.NewDecoder(resp.Body).Decode(&leaseResp)
	resp.Body.Close()

	if len(leaseResp.Jobs) != 1 {
		t.Fatalf("expected 1 leased job, got %d", len(leaseResp.Jobs))
	}
	leasedItem := leaseResp.Jobs[0]
	if leasedItem.LeaseToken != 1 {
		t.Fatalf("expected token 1, got %d", leasedItem.LeaseToken)
	}

	// 4. Heartbeat
	hbBody := []byte(`{"worker_id":"worker-1","lease_token":1}`)
	req, _ = http.NewRequest("POST", ts.URL+"/v1/jobs/"+submitResp.ID+"/heartbeat", bytes.NewReader(hbBody))
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for heartbeat, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 5. Complete
	compBody := []byte(`{"worker_id":"worker-1","lease_token":1,"success":true}`)
	req, _ = http.NewRequest("POST", ts.URL+"/v1/jobs/"+submitResp.ID+"/complete", bytes.NewReader(compBody))
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for complete, got %d", resp.StatusCode)
	}
	var compResp CompleteResponse
	_ = json.NewDecoder(resp.Body).Decode(&compResp)
	resp.Body.Close()
	if compResp.State != "succeeded" {
		t.Fatalf("expected state succeeded, got %s", compResp.State)
	}
}

func TestAPI_BearerAuth(t *testing.T) {
	srv, eng, _ := setupTestServer(t, "top-secret")
	defer eng.Close()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. No token -> 401
	resp, err := ts.Client().Get(ts.URL + "/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", resp.StatusCode)
	}

	// 2. Wrong token -> 401
	req, _ := http.NewRequest("GET", ts.URL+"/v1/stats", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	resp, err = ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", resp.StatusCode)
	}

	// 3. Valid token -> 200
	req, _ = http.NewRequest("GET", ts.URL+"/v1/stats", nil)
	req.Header.Set("Authorization", "Bearer top-secret")
	resp, err = ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
}

func TestAPI_DisallowUnknownFields(t *testing.T) {
	srv, eng, _ := setupTestServer(t, "")
	defer eng.Close()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Unknown field "typo_field"
	body := []byte(`{"type":"email","queue":"default","typo_field":"oops"}`)
	resp, err := ts.Client().Post(ts.URL+"/v1/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for unknown fields, got %d", resp.StatusCode)
	}
}

func TestAPI_LongPollParkingLot(t *testing.T) {
	srv, eng, _ := setupTestServer(t, "")
	defer eng.Close()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	doneCh := make(chan struct{})
	var leaseResp LeaseResponse

	// Start long-poll in background
	go func() {
		leaseBody := []byte(`{"worker_id":"worker-longpoll","queues":["default"],"max":1,"lease_seconds":30,"wait_seconds":5}`)
		resp, err := ts.Client().Post(ts.URL+"/v1/lease", "application/json", bytes.NewReader(leaseBody))
		if err == nil {
			_ = json.NewDecoder(resp.Body).Decode(&leaseResp)
			resp.Body.Close()
		}
		close(doneCh)
	}()

	// Give goroutine a moment to park
	time.Sleep(50 * time.Millisecond)

	// Submit job to default queue -> wakes parking lot
	submitBody := []byte(`{"type":"async.task","queue":"default"}`)
	resp, err := ts.Client().Post(ts.URL+"/v1/jobs", "application/json", bytes.NewReader(submitBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Long poll should return in well under the 5s timeout
	select {
	case <-doneCh:
		if len(leaseResp.Jobs) != 1 {
			t.Fatalf("expected 1 job received from long poll, got %d", len(leaseResp.Jobs))
		}
		if leaseResp.Jobs[0].Job.Type != "async.task" {
			t.Fatalf("expected async.task, got %s", leaseResp.Jobs[0].Job.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll timed out without waking up")
	}
}

func TestAPI_DelayedJob_Scheduler(t *testing.T) {
	dir := t.TempDir()
	parking := NewParkingLot()
	baseTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	clock := core.NewFakeClock(baseTime)

	eng, err := engine.Open(engine.Config{
		DataDir: dir,
		Clock:   clock,
		WALOptions: wal.Options{Sync: true},
		OnJobEligible: func(queue string) {
			parking.Signal(queue)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	srv := NewServer(ServerConfig{
		Engine:     eng,
		ParkingLot: parking,
		Clock:      clock,
	})

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// Submit job with run_at = baseTime + 1 hour
	runAt := baseTime.Add(1 * time.Hour)
	jobBody, _ := json.Marshal(map[string]any{
		"type":   "delayed.job",
		"queue":  "default",
		"run_at": runAt,
	})

	resp, err := ts.Client().Post(ts.URL+"/v1/jobs", "application/json", bytes.NewReader(jobBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Immediate lease should return nothing because job is not yet due
	leaseBody := []byte(`{"worker_id":"w-immediate","queues":["default"],"max":1,"wait_seconds":0}`)
	resp, err = ts.Client().Post(ts.URL+"/v1/lease", "application/json", bytes.NewReader(leaseBody))
	if err != nil {
		t.Fatal(err)
	}
	var emptyResp LeaseResponse
	_ = json.NewDecoder(resp.Body).Decode(&emptyResp)
	resp.Body.Close()
	if len(emptyResp.Jobs) != 0 {
		t.Fatalf("expected 0 jobs immediately, got %d", len(emptyResp.Jobs))
	}

	// Advance fake clock by 2 hours -> job becomes due!
	clock.Advance(2 * time.Hour)

	// Lease now -> should retrieve delayed job
	resp, err = ts.Client().Post(ts.URL+"/v1/lease", "application/json", bytes.NewReader(leaseBody))
	if err != nil {
		t.Fatal(err)
	}
	var dueResp LeaseResponse
	_ = json.NewDecoder(resp.Body).Decode(&dueResp)
	resp.Body.Close()
	if len(dueResp.Jobs) != 1 || dueResp.Jobs[0].Job.Type != "delayed.job" {
		t.Fatalf("expected 1 delayed job after run_at arrived, got %v", dueResp.Jobs)
	}
}
