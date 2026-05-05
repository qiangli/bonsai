/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Audit log for dgraph2. Emits one JSON line per administrative or write
 * operation so an operator can answer "what changed and when" without
 * reading Badger logs.
 *
 * Use:
 *
 *	logger, err := audit.Open("/var/log/dgraph2-audit.log")
 *	defer logger.Close()
 *	db, _ := dgraph2.Open(dgraph2.Options{Dir: "...", AuditLog: logger})
 *
 * Each entry is a self-contained JSON object — no array, no header — so
 * standard log shippers (Vector, Fluent Bit, Promtail) can ingest it as a
 * stream without parser configuration.
 */

package audit

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// Logger writes audit entries as JSON lines to the configured writer.
// A nil *Logger is a valid no-op.
type Logger struct {
	mu     sync.Mutex
	w      io.Writer
	closer io.Closer // file handle when Open() was used; nil when New() was used
}

// New wraps an arbitrary io.Writer (stdout, *bytes.Buffer in tests, etc.).
// The caller owns the writer's lifetime.
func New(w io.Writer) *Logger {
	if w == nil {
		return nil
	}
	return &Logger{w: w}
}

// Open creates a logger backed by an append-mode file. The file is created
// with mode 0o640 if it doesn't exist. Close releases the file handle.
func Open(path string) (*Logger, error) {
	if path == "" {
		return nil, errors.New("audit.Open: path is required")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		return nil, err
	}
	return &Logger{w: f, closer: f}, nil
}

// Close releases the underlying file handle if Open() was used. No-op when
// the logger was constructed via New() with an externally-managed writer.
func (l *Logger) Close() error {
	if l == nil || l.closer == nil {
		return nil
	}
	return l.closer.Close()
}

// Entry is one audit record. Time, Operation, and Status are required; the
// rest are operation-specific.
type Entry struct {
	Time       time.Time      `json:"time"`
	Operation  string         `json:"op"`
	Status     string         `json:"status"`
	Namespace  uint64         `json:"namespace,omitempty"`
	Args       map[string]any `json:"args,omitempty"`
	Error      string         `json:"error,omitempty"`
	DurationMs int64          `json:"duration_ms,omitempty"`
}

// Log writes one entry. Nil-safe: Log on a nil *Logger is a cheap no-op so
// callers don't need to guard each call site.
func (l *Logger) Log(e Entry) {
	if l == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	if e.Status == "" {
		e.Status = "ok"
	}
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	data = append(data, '\n')
	l.mu.Lock()
	_, _ = l.w.Write(data)
	l.mu.Unlock()
}

// LogErr is a small convenience for the common "ran an op, capture
// duration, capture error if any" pattern. Pass start = time.Now() taken
// at the top of the operation.
func (l *Logger) LogErr(op string, namespace uint64, args map[string]any, start time.Time, err error) {
	if l == nil {
		return
	}
	entry := Entry{
		Operation:  op,
		Namespace:  namespace,
		Args:       args,
		DurationMs: time.Since(start).Milliseconds(),
	}
	if err != nil {
		entry.Status = "error"
		entry.Error = err.Error()
	}
	l.Log(entry)
}
