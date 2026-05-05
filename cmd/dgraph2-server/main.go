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
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/dgraph-io/dgo/v250/protos/api"
	apiproto "github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/qiangli/dgraph2/pkg/dgraph2"
)

func main() {
	dir := flag.String("dir", "./dgraph2-data", "data directory (Badger lives at <dir>/p)")
	addr := flag.String("http", ":8080", "HTTP listen address")
	grpcAddr := flag.String("grpc", ":9080", "gRPC listen address")
	flag.Parse()

	db, err := dgraph2.Open(dgraph2.Options{Dir: *dir})
	if err != nil {
		log.Fatalf("dgraph2.Open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Close: %v", err)
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/alter", handleAlter(db))
	mux.HandleFunc("/query", handleQuery(db))
	mux.HandleFunc("/mutate", handleMutate(db))
	mux.HandleFunc("/set", handleSet(db))
	mux.HandleFunc("/get", handleGet(db))
	mux.HandleFunc("/assign", handleAssign(db))
	mux.HandleFunc("/admin/backup", handleBackup(db))
	mux.HandleFunc("/admin/restore", handleRestore(db))
	mux.HandleFunc("/admin/schema", handleAdminSchema(db))
	mux.Handle("/metrics", promhttp.Handler())
	// /debug/pprof/* — registered as side effect of importing net/http/pprof.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	srv := &http.Server{Addr: *addr, Handler: mux}

	// gRPC server.
	gs := grpc.NewServer()
	api.RegisterDgraphServer(gs, newGRPCAdapter(db))
	gln, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("grpc listen %s: %v", *grpcAddr, err)
	}

	// Graceful shutdown on SIGINT/SIGTERM so the Badger Close runs.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
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
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("ListenAndServe: %v", err)
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

func handleBackup(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dst := r.URL.Query().Get("path")
		if dst == "" {
			http.Error(w, "path query param required", http.StatusBadRequest)
			return
		}
		if err := db.Backup(r.Context(), dst); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
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

func handleRestore(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		src := r.URL.Query().Get("path")
		if src == "" {
			http.Error(w, "path query param required", http.StatusBadRequest)
			return
		}
		if err := db.RestoreFrom(r.Context(), src); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	}
}
