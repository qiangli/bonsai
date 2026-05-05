/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/qiangli/bonsai/pkg/bonsai"
)

// TestServerHTTP exercises the bonsai-server HTTP endpoints end-to-end:
// Alter -> Assign -> Set -> Get, plus Health and Backup. The DB is opened
// against a t.TempDir() so each test starts clean; the HTTP layer is mounted
// under httptest so we don't need real port binding.
func TestServerHTTP(t *testing.T) {
	dir := t.TempDir()
	db, err := bonsai.Open(bonsai.Options{Dir: dir})
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

// TestServerAdmin exercises the new admin endpoints: /admin/state,
// /admin/draining, /admin/namespace, /admin/export.
func TestServerAdmin(t *testing.T) {
	dir := t.TempDir()
	db, err := bonsai.Open(bonsai.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	draining := &atomic.Bool{}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/alter", handleAlter(db))
	mux.HandleFunc("/mutate", handleMutate(db))
	mux.HandleFunc("/admin/state", handleAdminState(db))
	mux.HandleFunc("/admin/draining", handleAdminDraining(db, draining))
	mux.HandleFunc("/admin/namespace", handleAdminNamespace(db))
	mux.HandleFunc("/admin/export", handleExport(db))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Seed some data.
	mustPostJSON(t, srv.URL+"/alter", map[string]string{"schema": "name: string @index(exact) .\n"})
	mustPostBytes(t, srv.URL+"/mutate", "text/plain",
		[]byte("_:e <name> \"Exportable\" .\n"))

	// /admin/state
	resp := mustGet(t, srv.URL+"/admin/state")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(body, []byte(`"namespaces"`)) {
		t.Errorf("admin/state missing namespaces: %s", string(body))
	}

	// /admin/namespace POST + GET
	resp, _ = http.Post(srv.URL+"/admin/namespace?ns=7", "", nil)
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Errorf("create ns=7: %d %s", resp.StatusCode, string(b))
	}
	resp.Body.Close()

	resp = mustGet(t, srv.URL+"/admin/namespace")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(body, []byte("7")) {
		t.Errorf("ns=7 not listed: %s", string(body))
	}

	// /admin/draining toggle
	resp, _ = http.Post(srv.URL+"/admin/draining?on=1", "", nil)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(body, []byte(`"draining":true`)) {
		t.Errorf("draining toggle: %s", string(body))
	}

	// /admin/export RDF
	dst := dir + "/exported.rdf"
	resp, _ = http.Post(srv.URL+"/admin/export?format=rdf&path="+dst, "", nil)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("export: %d %s", resp.StatusCode, string(body))
	}
	exported, _ := os.ReadFile(dst)
	if !bytes.Contains(exported, []byte("Exportable")) {
		t.Errorf("exported RDF missing data: %s", string(exported))
	}
}

// TestServerOps exercises /metrics, /admin/schema GET, /debug/pprof.
func TestServerOps(t *testing.T) {
	dir := t.TempDir()
	db, err := bonsai.Open(bonsai.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/alter", handleAlter(db))
	mux.HandleFunc("/admin/schema", handleAdminSchema(db))
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Add some schema, then GET it back.
	mustPostJSON(t, srv.URL+"/alter",
		map[string]string{"schema": "name: string @index(exact) .\nage: int .\n"})

	resp := mustGet(t, srv.URL+"/admin/schema")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(body, []byte("name: string @index(exact)")) {
		t.Errorf("schema GET missing predicate: %s", string(body))
	}
	if !bytes.Contains(body, []byte("age: int")) {
		t.Errorf("schema GET missing age: %s", string(body))
	}

	// /metrics — should return Prometheus text exposition.
	resp = mustGet(t, srv.URL+"/metrics")
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Contains(body, []byte("# HELP")) {
		t.Errorf("/metrics not Prometheus format: %s", string(body[:min(200, len(body))]))
	}

	// /debug/pprof — index page.
	resp = mustGet(t, srv.URL+"/debug/pprof/")
	if resp.StatusCode != 200 {
		t.Errorf("pprof index status: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// TestServerGRPC exercises the gRPC api.DgraphServer adapter via the dgo
// client. Proves that a real Dgraph client can talk to bonsai-server
// unchanged.
func TestServerGRPC(t *testing.T) {
	dir := t.TempDir()
	db, err := bonsai.Open(bonsai.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Bind a random port and start the gRPC server in-process.
	gln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	api.RegisterDgraphServer(gs, newGRPCAdapter(db))
	go func() { _ = gs.Serve(gln) }()
	defer gs.GracefulStop()

	// dgo client.
	conn, err := grpc.NewClient(gln.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	ctx := context.Background()
	if err := dg.Alter(ctx, &api.Operation{Schema: "name: string @index(exact) .\n"}); err != nil {
		t.Fatalf("Alter via gRPC: %v", err)
	}

	txn := dg.NewTxn()
	resp, err := txn.Mutate(ctx, &api.Mutation{
		SetNquads: []byte(`_:dave <name> "Dave" .`),
	})
	if err != nil {
		t.Fatalf("Mutate via gRPC: %v", err)
	}
	if _, ok := resp.Uids["dave"]; !ok {
		t.Errorf("no uid for dave: %v", resp.Uids)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	q, err := dg.NewReadOnlyTxn().Query(ctx, `{ q(func: eq(name, "Dave")) { name } }`)
	if err != nil {
		t.Fatalf("Query via gRPC: %v", err)
	}
	if !bytes.Contains(q.Json, []byte("Dave")) {
		t.Errorf("query missed Dave: %s", string(q.Json))
	}
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
