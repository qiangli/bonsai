/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/qiangli/dgraph2/pkg/dgraph2"
)

// TestServerHTTP exercises the dgraph2-server HTTP endpoints end-to-end:
// Alter -> Assign -> Set -> Get, plus Health and Backup. The DB is opened
// against a t.TempDir() so each test starts clean; the HTTP layer is mounted
// under httptest so we don't need real port binding.
func TestServerHTTP(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/alter", handleAlter(db))
	mux.HandleFunc("/set", handleSet(db))
	mux.HandleFunc("/get", handleGet(db))
	mux.HandleFunc("/assign", handleAssign(db))
	mux.HandleFunc("/query", handleQuery(db))
	mux.HandleFunc("/mutate", handleMutate(db))
	mux.HandleFunc("/admin/backup", handleBackup(db))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// /health
	resp := mustGet(t, srv.URL+"/health")
	if resp.StatusCode != 200 {
		t.Fatalf("health: status %d", resp.StatusCode)
	}

	// /alter
	mustPostJSON(t, srv.URL+"/alter", map[string]string{"schema": "name: string @index(exact) .\n"})

	// /assign
	resp = mustGet(t, srv.URL+"/assign?n=1")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var assigned map[string]uint64
	if err := json.Unmarshal(body, &assigned); err != nil {
		t.Fatalf("decode assign response %q: %v", string(body), err)
	}
	uid := assigned["start"]
	if uid == 0 {
		t.Fatalf("assign returned 0 uid (body=%s)", string(body))
	}

	// /set
	mustPostJSON(t, srv.URL+"/set", map[string]any{
		"subject":   uid,
		"predicate": "name",
		"value":     "Charlie",
	})

	// /get
	resp = mustGet(t, srv.URL+"/get?uid="+strconv.FormatUint(uid, 10)+"&pred=name")
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("get: status %d, body=%s", resp.StatusCode, string(b))
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "Charlie" {
		t.Errorf("get: want %q, got %q", "Charlie", string(got))
	}

	// /mutate + /query: full RDF→DQL roundtrip via HTTP.
	mustPostBytes(t, srv.URL+"/mutate", "text/plain", []byte(
		"_:zoe <name> \"Zoe\" .\n"))
	resp = mustPostBytes(t, srv.URL+"/query", "application/dql",
		[]byte(`{ q(func: eq(name, "Zoe")) { uid name } }`))
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("query: status %d, body=%s", resp.StatusCode, string(b))
	}
	qbody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(qbody, []byte(`"name":"Zoe"`)) {
		t.Errorf("query response missing Zoe: %s", string(qbody))
	}

	// /admin/backup
	dst := dir + "/backup.bin"
	resp = mustGet(t, srv.URL+"/admin/backup?path="+dst)
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("backup: status %d, body=%s", resp.StatusCode, string(b))
	}
}

func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url) //nolint:gosec // test server URL
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

func mustPostBytes(t *testing.T, url, contentType string, body []byte) *http.Response {
	t.Helper()
	resp, err := http.Post(url, contentType, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func mustPostJSON(t *testing.T, url string, payload any) {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s: status %d, body=%s", url, resp.StatusCode, string(b))
	}
}
