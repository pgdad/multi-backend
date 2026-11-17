package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Fanout receives requests from Envoy (route "fanout"), sends each one to every enabled backend
// concurrently and returns the first response whose status is in the success range. Remaining
// backends are allowed to finish (up to the timeout) so that statistics are collected for all of
// them; one request record with a per-backend summary is then logged.
type Fanout struct {
	ctl    *Controller
	client *http.Client
}

var hopHeaders = map[string]bool{
	"Connection": true, "Proxy-Connection": true, "Keep-Alive": true, "Te": true, "Trailer": true,
	"Transfer-Encoding": true, "Upgrade": true, "Content-Length": true,
}

func NewFanout(ctl *Controller) *Fanout {
	tr := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     60 * time.Second,
		DisableCompression:  true,
	}
	return &Fanout{ctl: ctl, client: &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}}
}

type attempt struct {
	BackendResult
	header http.Header
	body   []byte
	done   time.Duration
}

func (f *Fanout) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rc := f.ctl.Current()
	reqID := r.Header.Get("X-Request-Id")
	if reqID == "" {
		reqID = newID()
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, int64(rc.Fanout.MaxBodyBytes)+1))
	if err != nil || len(body) > rc.Fanout.MaxBodyBytes {
		http.Error(w, "request body too large or unreadable for fanout", http.StatusRequestEntityTooLarge)
		return
	}
	var targets []Backend
	for _, b := range rc.Backends {
		if b.IsEnabled() {
			targets = append(targets, b)
		}
	}
	timeout := time.Duration(rc.Fanout.TimeoutMs) * time.Millisecond
	isSuccess := func(code int) bool { return code >= rc.Fanout.SuccessMin && code <= rc.Fanout.SuccessMax }

	results := make(chan *attempt, len(targets))
	for _, b := range targets {
		go func(b Backend) { results <- f.call(r, b, body, reqID, start, timeout, isSuccess) }(b)
	}

	var all []*attempt
	var winner, firstResponse *attempt
	for len(all) < len(targets) {
		a := <-results
		all = append(all, a)
		if a.Result == "success" {
			winner = a
			break
		}
		if firstResponse == nil && a.Status > 0 {
			firstResponse = a
		}
	}

	var status int
	var respondedAt time.Duration
	switch {
	case winner != nil:
		winner.Winner = true
		status = winner.Status
		f.writeResponse(w, winner, len(targets), reqID)
	case firstResponse != nil:
		// No backend succeeded: return the first HTTP response received.
		status = firstResponse.Status
		f.writeResponse(w, firstResponse, len(targets), reqID)
	default:
		status = http.StatusBadGateway
		res := make([]BackendResult, 0, len(all))
		for _, a := range all {
			res = append(res, a.BackendResult)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Mb-Fanout-Backends", strconv.Itoa(len(targets)))
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "no backend responded", "request_id": reqID, "backends": res})
	}
	respondedAt = time.Since(start)

	// Collect the stragglers in the background, then record + log one summary for the request.
	go func(all []*attempt) {
		for len(all) < len(targets) {
			all = append(all, <-results)
		}
		res := make([]BackendResult, 0, len(all))
		for _, a := range all {
			res = append(res, a.BackendResult)
		}
		winnerName, winnerMs := "", 0.0
		if winner != nil {
			winnerName, winnerMs = winner.Name, winner.DurationMs
		}
		f.ctl.stats.Record(RequestRecord{
			RequestID: reqID, Mode: ModeFanout, Method: r.Method, Path: r.URL.RequestURI(),
			Status: status, DurationMs: ms(respondedAt), TotalDurationMs: ms(time.Since(start)),
			ConfigVersion: rc.Version, Winner: winnerName, Backends: res,
			Summary: summarize(res, winnerName, winnerMs),
		})
	}(all)
}

func (f *Fanout) writeResponse(w http.ResponseWriter, a *attempt, n int, reqID string) {
	for k, vs := range a.header {
		if hopHeaders[k] {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("X-Mb-Fanout-Winner", a.Name)
	w.Header().Set("X-Mb-Fanout-Backends", strconv.Itoa(n))
	w.Header().Set("Content-Length", strconv.Itoa(len(a.body)))
	w.WriteHeader(a.Status)
	_, _ = w.Write(a.body)
}

func (f *Fanout) call(in *http.Request, b Backend, body []byte, reqID string, start time.Time, timeout time.Duration, isSuccess func(int) bool) *attempt {
	a := &attempt{BackendResult: BackendResult{Name: b.Name, Address: b.HostPort()}}
	// Detached from the client request: we keep collecting stats even after answering the client.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	url := "http://" + b.HostPort() + in.URL.RequestURI()
	req, err := http.NewRequestWithContext(ctx, in.Method, url, bytes.NewReader(body))
	if err != nil {
		a.Result, a.Error = "conn_error", err.Error()
		return a
	}
	for k, vs := range in.Header {
		if hopHeaders[k] || strings.HasPrefix(k, "X-Mb-Mode") || k == "X-Mb-Backend" {
			continue
		}
		req.Header[k] = vs
	}
	req.Host = in.Host
	req.Header.Set("X-Request-Id", reqID)
	req.Header.Set("X-Mb-Fanout", "true")
	resp, err := f.client.Do(req)
	if err == nil {
		a.body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		a.Status, a.header = resp.StatusCode, resp.Header
	}
	a.DurationMs = ms(time.Since(start))
	a.Bytes = len(a.body)
	switch {
	case err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded)):
		a.Result, a.Error = "timeout", fmt.Sprintf("no response within %s", timeout)
	case err != nil:
		a.Result, a.Error = "conn_error", err.Error()
	case isSuccess(a.Status):
		a.Result = "success"
	default:
		a.Result = "http_error"
	}
	return a
}

func ms(d time.Duration) float64 { return round2(float64(d.Microseconds()) / 1000) }
