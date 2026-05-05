/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package audit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/dgraph2/pkg/audit"
	"github.com/qiangli/dgraph2/pkg/dgraph2"
)

// TestNilLoggerNoOp confirms a nil *Logger is safe to call — the dgraph2
// hot path relies on this so callers don't need a guard.
func TestNilLoggerNoOp(t *testing.T) {
	var l *audit.Logger
	l.Log(audit.Entry{Operation: "x"})
	l.LogErr("x", 0, nil, time.Now(), nil)
	if err := l.Close(); err != nil {
		t.Errorf("nil Close: %v", err)
	}
}

// TestLogJSONLines writes a few entries to an in-memory buffer and parses
// them back to confirm the wire format is one self-contained JSON object
// per line.
func TestLogJSONLines(t *testing.T) {
	var buf bytes.Buffer
	l := audit.New(&buf)
	l.Log(audit.Entry{Operation: "Op1", Status: "ok"})
	l.LogErr("Op2", 7, map[string]any{"k": "v"}, time.Now().Add(-2*time.Second), errors.New("boom"))

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d:\n%s", len(lines), buf.String())
	}

	var e1 audit.Entry
	if err := json.Unmarshal([]byte(lines[0]), &e1); err != nil {
		t.Fatalf("parse line 1: %v", err)
	}
	if e1.Operation != "Op1" || e1.Status != "ok" {
		t.Errorf("line 1 = %+v", e1)
	}

	var e2 audit.Entry
	if err := json.Unmarshal([]byte(lines[1]), &e2); err != nil {
		t.Fatalf("parse line 2: %v", err)
	}
	if e2.Operation != "Op2" || e2.Status != "error" || e2.Error != "boom" {
		t.Errorf("line 2 = %+v", e2)
	}
	if e2.Namespace != 7 {
		t.Errorf("namespace = %d, want 7", e2.Namespace)
	}
	if e2.DurationMs < 1000 {
		t.Errorf("duration_ms = %d, want >=1000", e2.DurationMs)
	}
}

// TestDBHooks confirms dgraph2.Open + AuditLog wires real operations
// (Alter, Mutate, DropAll, CreateNamespace) through to the audit logger.
func TestDBHooks(t *testing.T) {
	var buf bytes.Buffer
	db, err := dgraph2.Open(dgraph2.Options{
		Dir:      t.TempDir(),
		AuditLog: audit.New(&buf),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := db.Alter(ctx, "name: string @index(exact) ."); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	if _, err := db.Mutate(ctx, &apiproto.Mutation{
		SetNquads: []byte(`_:a <name> "Alice" .`),
	}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	if err := db.CreateNamespace(ctx, 42); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}
	if err := db.DropAll(ctx); err != nil {
		t.Fatalf("DropAll: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		`"op":"Alter"`,
		`"op":"Mutate"`,
		`"op":"CreateNamespace"`,
		`"op":"DropAll"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("audit log missing %s\nfull log:\n%s", want, out)
		}
	}
	if !strings.Contains(out, `"status":"ok"`) {
		t.Errorf("expected at least one ok status: %s", out)
	}
}
