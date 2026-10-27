package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// Sink receives samples from the pollers. Write is only called from the
// single writer goroutine, so implementations need no locking.
type Sink interface {
	Write(Sample)
	// Flush pushes buffered samples out. It is called on a timer, when the
	// sample queue drains, and once at shutdown.
	Flush()
}

// runWriter drains the sample channel into the sink until the channel closes.
func runWriter(samples <-chan Sample, sink Sink, flushEvery time.Duration) {
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()
	for {
		select {
		case s, ok := <-samples:
			if !ok {
				sink.Flush()
				return
			}
			sink.Write(s)
			if _, isStdout := sink.(*stdoutSink); isStdout && len(samples) == 0 {
				sink.Flush()
			}
		case <-ticker.C:
			sink.Flush()
		}
	}
}

// ---------------------------------------------------------------- stdout

type stdoutSink struct {
	w    *bufio.Writer
	enc  *json.Encoder
	json bool
}

func newStdoutSink(format string) *stdoutSink {
	w := bufio.NewWriter(os.Stdout)
	return &stdoutSink{w: w, enc: json.NewEncoder(w), json: format == "json"}
}

func (s *stdoutSink) Write(sm Sample) {
	if s.json {
		_ = s.enc.Encode(sm)
		return
	}
	name := sm.Name
	if sm.Index != "" {
		name += "." + sm.Index
	}
	fmt.Fprintf(s.w, "%s %s %s %s = (%s) %v\n",
		sm.Time.Format("2006-01-02T15:04:05"), sm.Device, sm.Group, name, sm.Type, sm.Value)
}

func (s *stdoutSink) Flush() { _ = s.w.Flush() }

// ---------------------------------------------------------------- http

// httpSink batches samples and POSTs them as a JSON array to the ingest
// server. Batches are retried a few times on transport or 5xx errors and
// dropped (with a log line) after that, so a dead collector never blocks
// polling. Samples are never buffered beyond maxPending.
type httpSink struct {
	cfg     OutputConfig
	client  *http.Client
	log     *slog.Logger
	pending []Sample
	sent    uint64
	dropped uint64
}

const (
	httpMaxAttempts = 4
	httpMaxPending  = 100_000
)

func newHTTPSink(cfg OutputConfig, log *slog.Logger) *httpSink {
	return &httpSink{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout},
		log:    log.With("sink", "http", "url", cfg.URL),
	}
}

func (h *httpSink) Write(s Sample) {
	if len(h.pending) >= httpMaxPending {
		h.dropped++
		return
	}
	h.pending = append(h.pending, s)
	if len(h.pending) >= h.cfg.BatchSize {
		h.Flush()
	}
}

func (h *httpSink) Flush() {
	if len(h.pending) == 0 {
		return
	}
	batch := h.pending
	h.pending = make([]Sample, 0, h.cfg.BatchSize)
	if err := h.send(batch); err != nil {
		h.dropped += uint64(len(batch))
		h.log.Error("batch dropped", "samples", len(batch), "dropped_total", h.dropped, "err", err)
		return
	}
	h.sent += uint64(len(batch))
	h.log.Debug("batch sent", "samples", len(batch), "sent_total", h.sent)
}

func (h *httpSink) send(batch []Sample) error {
	body, err := h.encode(batch)
	if err != nil {
		return err
	}
	var lastErr error
	for attempt := 1; attempt <= httpMaxAttempts; attempt++ {
		retry, err := h.post(body)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retry {
			return err
		}
		backoff := time.Duration(attempt*attempt) * 500 * time.Millisecond
		h.log.Warn("post failed, retrying", "attempt", attempt, "backoff", backoff, "err", err)
		time.Sleep(backoff)
	}
	return lastErr
}

func (h *httpSink) encode(batch []Sample) ([]byte, error) {
	raw, err := json.Marshal(batch)
	if err != nil {
		return nil, err
	}
	if !h.cfg.Compress() {
		return raw, nil
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(raw); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// post returns (retryable, error). 5xx and 429 are retryable; other 4xx are not.
func (h *httpSink) post(body []byte) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "fs-snmp")
	if h.cfg.Compress() {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if h.cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+h.cfg.Token)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		return true, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return false, nil
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	retry := resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests
	return retry, fmt.Errorf("http %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
}
