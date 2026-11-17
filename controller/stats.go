package main

import (
	"encoding/json"
	"math"
	"sort"
	"sync"
	"time"
)

// BackendResult is the outcome of one request attempt against one backend.
type BackendResult struct {
	Name       string  `json:"name"`
	Address    string  `json:"address,omitempty"`
	Status     int     `json:"status"`
	DurationMs float64 `json:"duration_ms"`
	Result     string  `json:"result"` // success | http_error | conn_error | timeout
	Error      string  `json:"error,omitempty"`
	Bytes      int     `json:"bytes,omitempty"`
	Winner     bool    `json:"winner,omitempty"`
}

// RequestRecord is one line in the request log: one client request and every backend it touched.
type RequestRecord struct {
	Type            string          `json:"type"`
	TS              string          `json:"ts"`
	RequestID       string          `json:"request_id"`
	Mode            string          `json:"mode"`
	Method          string          `json:"method"`
	Path            string          `json:"path"`
	Status          int             `json:"status"`
	DurationMs      float64         `json:"duration_ms"`
	TotalDurationMs float64         `json:"total_duration_ms,omitempty"`
	ResponseFlags   string          `json:"response_flags,omitempty"`
	ConfigVersion   int64           `json:"config_version,omitempty"`
	Winner          string          `json:"winner,omitempty"`
	Backends        []BackendResult `json:"backends"`
	Summary         *FanoutSummary  `json:"summary,omitempty"`
}

// FanoutSummary condenses the per-backend outcomes of a single fanned-out request.
type FanoutSummary struct {
	Backends    int            `json:"backends"`
	Succeeded   int            `json:"succeeded"`
	Failed      int            `json:"failed"`
	Winner      string         `json:"winner,omitempty"`
	WinnerMs    float64        `json:"winner_ms,omitempty"`
	Fastest     string         `json:"fastest,omitempty"`
	Slowest     string         `json:"slowest,omitempty"`
	SlowestMs   float64        `json:"slowest_ms,omitempty"`
	StatusCodes map[string]int `json:"status_codes"`
}

const latencyWindow = 4096

type backendStats struct {
	Requests   uint64
	Success    uint64
	Failure    uint64
	ConnErrors uint64
	Timeouts   uint64
	Status     map[string]uint64
	ByMode     map[string]uint64
	FanoutWins uint64
	LastStatus int
	LastSeen   time.Time
	latSum     float64
	latMax     float64
	lat        []float64
	latIdx     int
}

func (b *backendStats) add(r BackendResult, mode string, success bool) {
	b.Requests++
	b.ByMode[mode]++
	switch {
	case r.Result == "conn_error":
		b.ConnErrors++
		b.Failure++
	case r.Result == "timeout":
		b.Timeouts++
		b.Failure++
	case success:
		b.Success++
	default:
		b.Failure++
	}
	if r.Status > 0 {
		b.Status[statusClass(r.Status)]++
		b.LastStatus = r.Status
	}
	if r.Winner {
		b.FanoutWins++
	}
	b.LastSeen = time.Now()
	b.latSum += r.DurationMs
	if r.DurationMs > b.latMax {
		b.latMax = r.DurationMs
	}
	if len(b.lat) < latencyWindow {
		b.lat = append(b.lat, r.DurationMs)
	} else {
		b.lat[b.latIdx] = r.DurationMs
		b.latIdx = (b.latIdx + 1) % latencyWindow
	}
}

func statusClass(code int) string {
	return string(rune('0'+code/100)) + "xx"
}

// Stats aggregates per-backend and per-mode statistics and keeps a ring of recent requests.
type Stats struct {
	mu        sync.Mutex
	since     time.Time
	backends  map[string]*backendStats
	modes     map[string]uint64
	fanout    struct{ Requests, NoSuccess uint64 }
	recent    []RequestRecord
	recentIdx int
	log       *rotatingFile
	logStdout bool
	successOK func(int) bool
}

func NewStats(log *rotatingFile, logStdout bool, successOK func(int) bool) *Stats {
	return &Stats{since: time.Now(), backends: map[string]*backendStats{}, modes: map[string]uint64{},
		log: log, logStdout: logStdout, successOK: successOK}
}

func (s *Stats) backend(name string) *backendStats {
	b, ok := s.backends[name]
	if !ok {
		b = &backendStats{Status: map[string]uint64{}, ByMode: map[string]uint64{}}
		s.backends[name] = b
	}
	return b
}

// Record stores the statistics of one request and writes it to the request log.
func (s *Stats) Record(rec RequestRecord) {
	if rec.TS == "" {
		rec.TS = time.Now().UTC().Format(time.RFC3339Nano)
	}
	rec.Type = "request"
	s.mu.Lock()
	s.modes[rec.Mode]++
	if rec.Mode == ModeFanout {
		s.fanout.Requests++
		if rec.Summary != nil && rec.Summary.Succeeded == 0 {
			s.fanout.NoSuccess++
		}
	}
	for _, r := range rec.Backends {
		s.backend(r.Name).add(r, rec.Mode, r.Result == "success")
	}
	if len(s.recent) < 1000 {
		s.recent = append(s.recent, rec)
	} else {
		s.recent[s.recentIdx] = rec
		s.recentIdx = (s.recentIdx + 1) % len(s.recent)
	}
	s.mu.Unlock()

	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	if s.log != nil {
		s.log.WriteLine(data)
	}
	if s.logStdout {
		stdoutMu.Lock()
		stdoutWrite(data)
		stdoutMu.Unlock()
	}
}

// Recent returns up to n most recent request records, newest first.
func (s *Stats) Recent(n int) []RequestRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]RequestRecord, 0, n)
	total := len(s.recent)
	for i := 0; i < total && len(out) < n; i++ {
		idx := (s.recentIdx - 1 - i + 2*total) % total
		out = append(out, s.recent[idx])
	}
	return out
}

func (s *Stats) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.since = time.Now()
	s.backends = map[string]*backendStats{}
	s.modes = map[string]uint64{}
	s.fanout.Requests, s.fanout.NoSuccess = 0, 0
	s.recent, s.recentIdx = nil, 0
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	return round2(sorted[idx])
}

func round2(f float64) float64 { return math.Round(f*100) / 100 }

// Snapshot returns a JSON-friendly view of all statistics.
func (s *Stats) Snapshot() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total uint64
	modes := map[string]uint64{}
	for k, v := range s.modes {
		modes[k] = v
		total += v
	}
	backends := map[string]any{}
	names := make([]string, 0, len(s.backends))
	for n := range s.backends {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		b := s.backends[n]
		lat := append([]float64(nil), b.lat...)
		sort.Float64s(lat)
		avg := 0.0
		if b.Requests > 0 {
			avg = b.latSum / float64(b.Requests)
		}
		share := 0.0
		if total > 0 {
			share = round2(100 * float64(b.ByMode[ModeRoundRobin]+b.ByMode[ModeWeighted]+b.ByMode["direct"]) / float64(total))
		}
		entry := map[string]any{
			"requests": b.Requests, "success": b.Success, "failure": b.Failure,
			"conn_errors": b.ConnErrors, "timeouts": b.Timeouts,
			"status_classes": copyMap(b.Status), "by_mode": copyMap(b.ByMode),
			"fanout_wins": b.FanoutWins, "last_status": b.LastStatus,
			"balanced_share_pct": share,
			"latency_ms": map[string]float64{
				"avg": round2(avg), "p50": percentile(lat, 50), "p90": percentile(lat, 90),
				"p99": percentile(lat, 99), "max": round2(b.latMax),
			},
		}
		if !b.LastSeen.IsZero() {
			entry["last_seen"] = b.LastSeen.UTC().Format(time.RFC3339Nano)
		}
		backends[n] = entry
	}
	return map[string]any{
		"since":          s.since.UTC().Format(time.RFC3339),
		"total_requests": total,
		"by_mode":        modes,
		"fanout":         map[string]uint64{"requests": s.fanout.Requests, "no_success": s.fanout.NoSuccess},
		"backends":       backends,
	}
}

func copyMap(m map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// summarize builds the per-request fanout summary.
func summarize(results []BackendResult, winner string, winnerMs float64) *FanoutSummary {
	s := &FanoutSummary{Backends: len(results), Winner: winner, WinnerMs: winnerMs, StatusCodes: map[string]int{}}
	fastest := math.MaxFloat64
	for _, r := range results {
		if r.Result == "success" {
			s.Succeeded++
			if r.DurationMs < fastest {
				fastest, s.Fastest = r.DurationMs, r.Name
			}
		} else {
			s.Failed++
		}
		if r.DurationMs >= s.SlowestMs {
			s.SlowestMs, s.Slowest = r.DurationMs, r.Name
		}
		key := r.Result
		if r.Status > 0 {
			key = itoa(r.Status)
		}
		s.StatusCodes[key]++
	}
	return s
}
