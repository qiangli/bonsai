/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// dgraph2-server is the thin CLI/HTTP wrapper around pkg/dgraph2.
//
// It opens an embedded dgraph2 DB and exposes the smoke-test surface over
// HTTP: schema alter, single-triple set/get, backup, restore. The full DQL
// gRPC surface (worker.MutateOverNetwork / ProcessTaskOverNetwork) is still
// stubbed in the worker package; once those are wired this binary will grow
// to register `api.DgraphServer` and the rest of the upstream HTTP routes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/dgo/v250/protos/api"
	apiproto "github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	flag "github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/qiangli/dgraph2/pkg/audit"
	"github.com/qiangli/dgraph2/pkg/dgraph2"
	"github.com/qiangli/dgraph2/pkg/graphql"
	"github.com/qiangli/dgraph2/pkg/ui"
)

// version is set at build time via -ldflags "-X main.version=...". It's
// returned by the gRPC CheckVersion RPC and tagged onto OTel spans.
var version = "dev"

// serverConfig is the effective configuration after flag parsing. It backs
// /admin/config for read-only introspection. Pointer fields (draining) reflect
// runtime-mutable state; everything else is boot-time fixed.
type serverConfig struct {
	Version       string `json:"version"`
	Dir           string `json:"dir"`
	HTTPAddr      string `json:"http"`
	GRPCAddr      string `json:"grpc"`
	TLSEnabled    bool   `json:"tls_enabled"`
	TraceStdout   bool   `json:"trace_stdout"`
	TraceOTLPHTTP bool   `json:"trace_otlp_http"`
	TraceOTLPGRPC bool   `json:"trace_otlp_grpc"`
	TraceEndpoint string `json:"trace_endpoint,omitempty"`
	ServiceName   string `json:"trace_service_name"`
	Draining      bool   `json:"draining"`
}

func main() {
	configFile := flag.String("config", "", "YAML config file (overridden by env DGRAPH2_* and CLI flags)")
	dir := flag.String("dir", "./dgraph2-data", "data directory (Badger lives at <dir>/p)")
	addr := flag.String("http", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc", ":9080", "gRPC listen address")
	tlsCert := flag.String("tls-cert", "", "PEM-encoded TLS cert (enables TLS on both HTTP and gRPC)")
	tlsKey := flag.String("tls-key", "", "PEM-encoded TLS private key")
	traceStdout := flag.Bool("trace-stdout", false, "emit OpenTelemetry traces to stdout")
	traceOTLPHTTP := flag.Bool("trace-otlp-http", false, "emit OpenTelemetry traces via OTLP/HTTP")
	traceOTLPGRPC := flag.Bool("trace-otlp-grpc", false, "emit OpenTelemetry traces via OTLP/gRPC")
	traceJaeger := flag.String("trace-jaeger", "", "send traces to a Jaeger collector at host:port (Jaeger 1.35+ accepts OTLP/gRPC natively); shortcut for --trace-otlp-grpc --trace-endpoint <host:port> --trace-insecure")
	traceEndpoint := flag.String("trace-endpoint", "", "OTLP exporter endpoint host:port (default: localhost:4318 for HTTP, localhost:4317 for gRPC; OTEL_EXPORTER_OTLP_ENDPOINT also honoured)")
	traceInsecure := flag.Bool("trace-insecure", true, "skip TLS for OTLP exporters")
	traceServiceName := flag.String("trace-service-name", "dgraph2", "service.name resource attribute")
	auditLogPath := flag.String("audit-log", "", "audit log file path (JSON lines); empty disables auditing")
	flag.Parse()

	// Wire viper for env (DGRAPH2_*) + optional YAML config.
	// Precedence: explicit CLI flag > env > YAML > flag default.
	if err := loadConfig(*configFile); err != nil {
		log.Fatalf("config: %v", err)
	}

	// --trace-jaeger is a preset: enable OTLP/gRPC to the supplied endpoint
	// with TLS disabled, since Jaeger collectors typically run on the local
	// network. Explicit --trace-otlp-grpc / --trace-endpoint still wins
	// when set together with --trace-jaeger.
	if *traceJaeger != "" {
		*traceOTLPGRPC = true
		*traceInsecure = true
		if *traceEndpoint == "" {
			*traceEndpoint = *traceJaeger
		}
	}

	// Wire an OpenTelemetry tracer provider. dgraph2 has otel imports
	// throughout query/ and worker/; without an exporter the spans are no-ops.
	shutdownTrace, err := setupTracing(context.Background(), tracingOpts{
		stdout:      *traceStdout,
		otlpHTTP:    *traceOTLPHTTP,
		otlpGRPC:    *traceOTLPGRPC,
		endpoint:    *traceEndpoint,
		insecure:    *traceInsecure,
		serviceName: *traceServiceName,
		version:     version,
	})
	if err != nil {
		log.Fatalf("tracing setup: %v", err)
	}
	defer func() { _ = shutdownTrace(context.Background()) }()
	if *traceStdout || *traceOTLPHTTP || *traceOTLPGRPC {
		log.Printf("OpenTelemetry tracing enabled (stdout=%v otlp-http=%v otlp-grpc=%v endpoint=%q)",
			*traceStdout, *traceOTLPHTTP, *traceOTLPGRPC, *traceEndpoint)
	}

	var auditLogger *audit.Logger
	if *auditLogPath != "" {
		auditLogger, err = audit.Open(*auditLogPath)
		if err != nil {
			log.Fatalf("audit.Open %s: %v", *auditLogPath, err)
		}
		defer func() { _ = auditLogger.Close() }()
		log.Printf("audit log: %s", *auditLogPath)
	}

	db, err := dgraph2.Open(dgraph2.Options{Dir: *dir, AuditLog: auditLogger})
	if err != nil {
		log.Fatalf("dgraph2.Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Close: %v", err)
		}
	}()

	// draining is set by /admin/draining; while true, /mutate and /alter
	// reject with 503 so the operator can drain in-flight work cleanly.
	draining := &atomic.Bool{}

	// stop is signalled by /admin/shutdown and OS signals.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/alter", handleAlter(db))
	mux.HandleFunc("/query", handleQuery(db))
	mux.HandleFunc("/mutate", handleMutate(db))
	mux.HandleFunc("/set", handleSet(db))
	mux.HandleFunc("/get", handleGet(db))
	mux.HandleFunc("/assign", handleAssign(db))
	mux.HandleFunc("/commit", handleCommit())
	mux.HandleFunc("/abort", handleAbort())
	mux.HandleFunc("/graphql", graphql.Handler(db))
	mux.HandleFunc("/graphql/subscribe", graphql.SubscriptionHandler(db))
	mux.Handle("/ui", ui.Handler())
	mux.Handle("/ui/", ui.Handler())
	mux.Handle("/", ui.Handler())
	mux.HandleFunc("/admin/backup", handleBackup(db))
	mux.HandleFunc("/admin/restore", handleRestore(db))
	mux.HandleFunc("/admin/export", handleExport(db))
	mux.HandleFunc("/admin/import", handleImport(db))
	mux.HandleFunc("/admin/schema", handleAdminSchema(db))
	mux.HandleFunc("/admin/state", handleAdminState(db))
	mux.HandleFunc("/admin/draining", handleAdminDraining(db, draining))
	mux.HandleFunc("/admin/shutdown", handleAdminShutdown(stop))
	mux.HandleFunc("/admin/namespace", handleAdminNamespace(db))
	mux.HandleFunc("/admin/config", handleAdminConfig(serverConfig{
		Version:       version,
		Dir:           *dir,
		HTTPAddr:      *addr,
		GRPCAddr:      *grpcAddr,
		TLSEnabled:    *tlsCert != "" && *tlsKey != "",
		TraceStdout:   *traceStdout,
		TraceOTLPHTTP: *traceOTLPHTTP,
		TraceOTLPGRPC: *traceOTLPGRPC,
		TraceEndpoint: *traceEndpoint,
		ServiceName:   *traceServiceName,
	}, draining))
	mux.Handle("/metrics", promhttp.Handler())
	// /debug/pprof/* — registered as side effect of importing net/http/pprof.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// HTTP server with sane production-grade timeouts.
	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       90 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MiB
	}

	// gRPC server.
	var grpcOpts []grpc.ServerOption
	if *tlsCert != "" && *tlsKey != "" {
		creds, err := credentials.NewServerTLSFromFile(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("load TLS: %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}
	gs := grpc.NewServer(grpcOpts...)
	api.RegisterDgraphServer(gs, newGRPCAdapter(db))
	gln, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen %s: %v", *grpcAddr, err)
	}

	_ = draining // suppress unused warning if no mutator code reads it
	go func() {
		<-stop
		log.Println("shutting down ...")
		gs.GracefulStop()
		_ = srv.Shutdown(context.Background())
	}()

	go func() {
		log.Printf("dgraph2-server gRPC listening on %s", *grpcAddr)
		if err := gs.Serve(gln); err != nil {
			log.Printf("gRPC Serve: %v", err)
		}
	}()

	log.Printf("dgraph2-server HTTP listening on %s, data at %s", *addr, *dir)
	var httpErr error
	if *tlsCert != "" && *tlsKey != "" {
		httpErr = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
	} else {
		httpErr = srv.ListenAndServe()
	}
	if httpErr != nil && !errors.Is(httpErr, http.ErrServerClosed) {
		log.Fatalf("ListenAndServe: %v", httpErr)
	}
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, `{"status":"ok"}`)
}

type alterReq struct {
	Schema string `json:"schema"`
}

func handleAlter(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req alterReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}
		if err := db.Alter(r.Context(), req.Schema); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	}
}

type setReq struct {
	Subject   uint64 `json:"subject"`
	Predicate string `json:"predicate"`
	Value     any    `json:"value"`
}

func handleSet(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req setReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode: %v", err), http.StatusBadRequest)
			return
		}
		if err := db.Set(r.Context(), req.Subject, req.Predicate, req.Value); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	}
}

func handleGet(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		uidStr := r.URL.Query().Get("uid")
		pred := r.URL.Query().Get("pred")
		if uidStr == "" || pred == "" {
			http.Error(w, "uid and pred query params required", http.StatusBadRequest)
			return
		}
		uid, err := strconv.ParseUint(uidStr, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("uid: %v", err), http.StatusBadRequest)
			return
		}
		val, err := db.Get(r.Context(), uid, pred)
		if errors.Is(err, dgraph2.ErrNoValue) {
			http.Error(w, "no value", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(val)
	}
}

// handleQuery accepts a DQL query as the request body and returns the JSON
// response on the wire (so curl just sees the result, not a wrapper object).
func handleQuery(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
			return
		}
		resp, err := db.Query(r.Context(), string(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(resp.Json)
	}
}

// handleMutate accepts an RDF SetNquads body (text/plain) and applies it.
func handleMutate(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)
			return
		}
		resp, err := db.Mutate(r.Context(), apiMutation{SetNquads: body}.toApi())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"uids": resp.Uids,
			"txn": map[string]uint64{
				"start_ts":  resp.Txn.GetStartTs(),
				"commit_ts": resp.Txn.GetCommitTs(),
			},
		})
	}
}

// apiMutation is a tiny shim so we don't have to import the dgo api package
// at this layer. The test/dev usage is `curl --data-binary @triples.rdf`.
type apiMutation struct {
	SetNquads []byte
	DelNquads []byte
}

func (m apiMutation) toApi() *apiproto.Mutation {
	return &apiproto.Mutation{SetNquads: m.SetNquads, DelNquads: m.DelNquads}
}

// handleCommit and handleAbort are compatibility shims. dgraph2 commits
// synchronously inside /mutate (no client-side txn state), so a follow-up
// /commit is a no-op and /abort always reports success — there is no
// pending state to roll back. Provided so dgo-style clients and proxies
// that issue these calls don't error out.
func handleCommit() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Echo the start_ts (if supplied) back as commit_ts so clients can
		// thread their own bookkeeping. Keys are inert here.
		var req struct {
			StartTs uint64   `json:"start_ts"`
			Keys    []string `json:"keys"`
			Preds   []string `json:"preds"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req) // body is optional
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"start_ts":  req.StartTs,
			"commit_ts": req.StartTs,
			"aborted":   false,
		})
	}
}

func handleAbort() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"aborted":true}`)
	}
}

func handleAssign(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nStr := r.URL.Query().Get("n")
		if nStr == "" {
			nStr = "1"
		}
		n, err := strconv.ParseUint(nStr, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("n: %v", err), http.StatusBadRequest)
			return
		}
		start, end, err := db.AssignUid(r.Context(), n)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]uint64{"start": start, "end": end})
	}
}

// handleBackup writes a backup. Two formats are supported:
//
//	POST /admin/backup?path=<file>                    single-file Badger Stream (default)
//	POST /admin/backup?path=<dir>&format=manifest     upstream-compatible multi-file format
//	POST /admin/backup?path=<dir>&format=manifest&type=incremental
//
// The manifest format produces a directory layout that upstream `dgraph
// restore` can consume.
func handleBackup(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dst := r.URL.Query().Get("path")
		if dst == "" {
			http.Error(w, "path query param required", http.StatusBadRequest)
			return
		}
		switch r.URL.Query().Get("format") {
		case "", "stream":
			if err := db.Backup(r.Context(), dst); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = io.WriteString(w, `{"status":"ok","format":"stream"}`)
		case "manifest":
			t := dgraph2.BackupFull
			if r.URL.Query().Get("type") == "incremental" {
				t = dgraph2.BackupIncremental
			}
			man, err := db.BackupTo(r.Context(), dgraph2.BackupOptions{Dir: dst, Type: t})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":     "ok",
				"format":     "manifest",
				"backup_id":  man.BackupID,
				"backup_num": man.BackupNum,
				"read_ts":    man.ReadTs,
				"path":       man.Path,
			})
		default:
			http.Error(w, "format must be stream or manifest", http.StatusBadRequest)
		}
	}
}

// handleExport runs a database export. Two modes:
//
//	POST /admin/export?format=rdf|json&path=/tmp/export.rdf    write to a server-local path
//	GET  /admin/export?format=rdf|json&download=true           stream the dump back in the response body
func handleExport(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		format := r.URL.Query().Get("format")
		if format == "" {
			format = "rdf"
		}
		download := r.URL.Query().Get("download") == "true" || r.URL.Query().Get("download") == "1"
		if download {
			ext := "rdf"
			ctype := "application/n-quads"
			if format == "json" {
				ext = "json"
				ctype = "application/json"
			}
			w.Header().Set("Content-Type", ctype)
			w.Header().Set("Content-Disposition",
				fmt.Sprintf(`attachment; filename="dgraph2-export.%s"`, ext))
			if err := db.ExportTo(r.Context(), format, w); err != nil {
				// Headers are already sent; best effort error trailer.
				_, _ = io.WriteString(w, "\n# export error: "+err.Error()+"\n")
			}
			return
		}
		dst := r.URL.Query().Get("path")
		if dst == "" {
			http.Error(w, "path query param required (or set download=true to stream)", http.StatusBadRequest)
			return
		}
		if err := db.Export(r.Context(), format, dst); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	}
}

// handleImport ingests RDF or JSON streamed in the POST body. Acts like
// dgraph2-bulk but driven by an HTTP upload — useful for tooling and CI
// pipelines that have an artifact rather than a server-local path.
//
//	POST /admin/import?format=rdf|json[&batch=1000]
//	  body: contents of the .rdf / .json file (gzip not unwrapped here)
func handleImport(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		format := r.URL.Query().Get("format")
		if format == "" {
			format = "rdf"
		}
		batch := 1000
		if v := r.URL.Query().Get("batch"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				http.Error(w, "batch must be a positive integer", http.StatusBadRequest)
				return
			}
			batch = n
		}
		summary, err := dgraph2.ImportStream(r.Context(), db, format, r.Body, batch)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(summary)
	}
}

// handleAdminState reports a small JSON state summary. dgraph2 has no
// cluster, so the shape is minimal: namespaces, current ts, max uid.
func handleAdminState(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nss, _ := db.ListNamespaces(r.Context())
		out := map[string]any{
			"namespaces": nss,
			"max_uid":    db.MaxUid(),
			"version":    "dgraph2-0.1.0",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}

// handleAdminDraining toggles the drain flag.
//   GET  /admin/draining        → {"draining": false}
//   POST /admin/draining?on=1   → flips on
func handleAdminDraining(db *dgraph2.DB, draining *atomic.Bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			on := r.URL.Query().Get("on") == "1" ||
				r.URL.Query().Get("on") == "true"
			draining.Store(on)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"draining": draining.Load()})
	}
}

// handleAdminConfig returns the boot-time effective config plus the live
// draining state. Read-only: mutating boot-time config requires a restart.
// Mutable runtime state (draining) lives behind /admin/draining.
func handleAdminConfig(cfg serverConfig, draining *atomic.Bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "GET only — runtime mutables go through /admin/{draining,namespace}", http.StatusMethodNotAllowed)
			return
		}
		out := cfg
		out.Draining = draining.Load()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}

// handleAdminShutdown signals the server to gracefully shut down.
func handleAdminShutdown(stop chan<- os.Signal) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"status":"shutting down"}`)
		go func() { stop <- syscall.SIGTERM }()
	}
}

// handleAdminNamespace: GET → list, POST?ns=N → create, DELETE?ns=N → drop.
func handleAdminNamespace(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			nss, err := db.ListNamespaces(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string][]uint64{"namespaces": nss})
		case http.MethodPost:
			ns, err := strconv.ParseUint(r.URL.Query().Get("ns"), 10, 64)
			if err != nil {
				http.Error(w, "ns query param required", http.StatusBadRequest)
				return
			}
			if err := db.CreateNamespace(r.Context(), ns); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = io.WriteString(w, `{"status":"ok"}`)
		case http.MethodDelete:
			ns, err := strconv.ParseUint(r.URL.Query().Get("ns"), 10, 64)
			if err != nil {
				http.Error(w, "ns query param required", http.StatusBadRequest)
				return
			}
			if err := db.DropNamespace(r.Context(), ns); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = io.WriteString(w, `{"status":"ok"}`)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// handleAdminSchema GETs dump the active schema in DQL form. POSTs reuse the
// existing Alter handler.
func handleAdminSchema(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			out, err := db.SchemaText(r.Context())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte(out))
		case http.MethodPost:
			handleAlter(db).ServeHTTP(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// handleRestore reads back a backup. Format is detected from the path:
//   - if <path> is a directory containing manifest.json, the upstream-
//     compatible multi-file format is applied (RestoreFromManifest).
//   - otherwise <path> is treated as a single-file Badger Stream backup
//     (RestoreFrom).
//
//	POST /admin/restore?path=<path>
func handleRestore(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		src := r.URL.Query().Get("path")
		if src == "" {
			http.Error(w, "path query param required", http.StatusBadRequest)
			return
		}
		if fi, err := os.Stat(src); err == nil && fi.IsDir() {
			if err := db.RestoreFromManifest(r.Context(), src); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = io.WriteString(w, `{"status":"ok","format":"manifest"}`)
			return
		}
		if err := db.RestoreFrom(r.Context(), src); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok","format":"stream"}`)
	}
}
