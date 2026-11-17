package main

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// AccessLogTailer follows Envoy's JSON access log and turns each balanced/direct request into a
// RequestRecord (fanout requests are recorded by the fanout proxy itself, with all backends).
// The file is rotated by the tailer when it grows too large (rename + Envoy /reopen_logs).
type AccessLogTailer struct {
	path    string
	ctl     *Controller
	maxSize int64
}

func (t *AccessLogTailer) Run() {
	var f *os.File
	var rd *bufio.Reader
	var ino uint64
	open := func() bool {
		nf, err := os.Open(t.path)
		if err != nil {
			return false
		}
		if f != nil {
			f.Close()
		}
		f, rd = nf, bufio.NewReaderSize(nf, 256*1024)
		ino = inode(nf)
		return true
	}
	for !open() {
		time.Sleep(200 * time.Millisecond)
	}
	f.Seek(0, io.SeekEnd)
	var partial string
	for {
		line, err := rd.ReadString('\n')
		if err == nil {
			t.handle(partial + line)
			partial = ""
			continue
		}
		partial += line
		// EOF: check rotation/truncation, then wait.
		if st, serr := os.Stat(t.path); serr == nil {
			if sysIno(st) != ino {
				// drain whatever remains in the old file, then switch
				rest, _ := io.ReadAll(rd)
				for _, l := range strings.SplitAfter(partial+string(rest), "\n") {
					if strings.HasSuffix(l, "\n") {
						t.handle(l)
					}
				}
				partial = ""
				open()
				continue
			}
			pos, _ := f.Seek(0, io.SeekCurrent)
			if st.Size() < pos-int64(rd.Buffered()) {
				f.Seek(0, io.SeekStart)
				rd.Reset(f)
				partial = ""
			}
			if st.Size() > t.maxSize {
				if os.Rename(t.path, t.path+".1") == nil {
					_ = t.ctl.envoy.Post("/reopen_logs")
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func inode(f *os.File) uint64 {
	st, err := f.Stat()
	if err != nil {
		return 0
	}
	return sysIno(st)
}

func sysIno(st os.FileInfo) uint64 {
	if s, ok := st.Sys().(*syscall.Stat_t); ok {
		return s.Ino
	}
	return 0
}

func num(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

func str(v any) string {
	switch x := v.(type) {
	case string:
		if x == "-" {
			return ""
		}
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case nil:
		return ""
	}
	return ""
}

func (t *AccessLogTailer) handle(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return
	}
	route := str(m["route"])
	cluster := str(m["upstream_cluster"])
	if route == "fanout" || cluster == fanoutCluster {
		return // recorded by the fanout proxy with full per-backend detail
	}
	mode := "unrouted"
	switch {
	case strings.HasPrefix(route, "balance:"):
		mode = strings.TrimPrefix(route, "balance:")
	case strings.HasPrefix(route, "direct:"):
		mode = "direct"
	}
	status := int(num(m["status"]))
	host := str(m["upstream_host"])
	flags := str(m["response_flags"])
	rec := RequestRecord{
		TS: str(m["start_time"]), RequestID: str(m["request_id"]), Mode: mode,
		Method: str(m["method"]), Path: str(m["path"]), Status: status,
		DurationMs: num(m["duration_ms"]), ResponseFlags: flags,
		Backends: []BackendResult{},
	}
	if v, err := strconv.ParseInt(str(m["config_version"]), 10, 64); err == nil {
		rec.ConfigVersion = v
	}
	if host != "" {
		name := t.ctl.backendNameFor(host, cluster)
		res := BackendResult{Name: name, Address: host, Status: status, DurationMs: num(m["upstream_ms"])}
		if res.DurationMs == 0 {
			res.DurationMs = rec.DurationMs
		}
		rc := t.ctl.Current()
		switch {
		case strings.Contains(flags, "UF") || strings.Contains(flags, "URX") || strings.Contains(flags, "UH"):
			res.Result, res.Error = "conn_error", "envoy flags "+flags
		case strings.Contains(flags, "UT"):
			res.Result, res.Error = "timeout", "envoy flags "+flags
		case status >= rc.Fanout.SuccessMin && status <= rc.Fanout.SuccessMax:
			res.Result = "success"
		default:
			res.Result = "http_error"
		}
		rec.Winner = name
		rec.Backends = append(rec.Backends, res)
	}
	t.ctl.stats.Record(rec)
}
