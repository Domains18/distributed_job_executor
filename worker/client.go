package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/domains18/kombucha/core"
)

var (
	ErrStaleToken = errors.New("worker: stale lease token (409 Conflict)")
	ErrNotFound   = errors.New("worker: not found (404)")
)

type LeaseItem struct {
	Job         core.Job  `json:"job"`
	LeaseToken  uint64    `json:"lease_token"`
	LeaseExpiry time.Time `json:"lease_expiry"`
}

type HeartbeatResponse struct {
	LeaseExpiry time.Time `json:"lease_expiry"`
	Cancelled   bool      `json:"cancelled"`
}

// Client communicates with the Kombucha coordinator over HTTP.
type Client struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
}

// NewClient creates a new coordinator API client.
func NewClient(baseURL, authToken string, timeout time.Duration) *Client {
	baseURL = strings.TrimSuffix(baseURL, "/")
	if timeout <= 0 {
		timeout = 35 * time.Second
	}
	return &Client{
		baseURL:   baseURL,
		authToken: authToken,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// Lease polls the coordinator for up to max jobs across queues.
func (c *Client) Lease(ctx context.Context, workerID string, queues []string, max, leaseSec, waitSec int) ([]LeaseItem, error) {
	body := map[string]any{
		"worker_id":     workerID,
		"queues":        queues,
		"max":           max,
		"lease_seconds": leaseSec,
		"wait_seconds":  waitSec,
	}
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v1/lease", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("worker: lease failed with status %d", resp.StatusCode)
	}

	var res struct {
		Jobs []LeaseItem `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}

	return res.Jobs, nil
}

// Heartbeat sends a lease extension and checks for cooperative cancellation.
func (c *Client) Heartbeat(ctx context.Context, jobID core.JobID, workerID string, token uint64, leaseSec int) (HeartbeatResponse, error) {
	body := map[string]any{
		"worker_id":     workerID,
		"lease_token":   token,
		"lease_seconds": leaseSec,
	}
	b, err := json.Marshal(body)
	if err != nil {
		return HeartbeatResponse{}, err
	}

	url := fmt.Sprintf("%s/v1/jobs/%s/heartbeat", c.baseURL, jobID.String())
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	if err != nil {
		return HeartbeatResponse{}, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return HeartbeatResponse{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return HeartbeatResponse{}, ErrStaleToken
	}
	if resp.StatusCode == http.StatusNotFound {
		return HeartbeatResponse{}, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return HeartbeatResponse{}, fmt.Errorf("worker: heartbeat failed with status %d", resp.StatusCode)
	}

	var res HeartbeatResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return HeartbeatResponse{}, err
	}

	return res, nil
}

// Complete reports success or failure of a leased job with the fencing token.
func (c *Client) Complete(ctx context.Context, jobID core.JobID, workerID string, token uint64, success bool, errMsg string, retryable bool) error {
	body := map[string]any{
		"worker_id":   workerID,
		"lease_token": token,
		"success":     success,
		"error":       errMsg,
		"retryable":   retryable,
	}
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/v1/jobs/%s/complete", c.baseURL, jobID.String())
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return ErrStaleToken
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker: complete failed with status %d", resp.StatusCode)
	}

	return nil
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	if c.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+c.authToken)
	}
}
