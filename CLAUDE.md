# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

**Bonsai** is a single-node, embeddable graph database — a carefully pruned fork of
upstream Dgraph. The cluster machinery (Zero, Raft, inter-alpha gRPC, group sharding,
distributed Oracle, ACL, at-rest encryption, Vault) has been removed. The DQL parser,
posting store, schema, indexing, and the worker query-execution engine are preserved.
The Go module is `github.com/qiangli/bonsai`; the public API lives at `pkg/bonsai/`.
A reference copy of upstream Dgraph lives at `priorart/dgraph/` (gitignored) and is the
source for porting work — read it, do not edit it.

The name comes from the metaphor: a bonsai is the same plant as the full-size tree,
trimmed to a manageable form while keeping the structure intact. That's the project.

Earlier history: this project was developed under the working name `dgraph2` and
renamed to `bonsai` in one pass; recent commits (`Wave 7d` and earlier) still
reference the old name in their messages.

## Build / test / run

```
make build      # builds the single `bonsai` binary
make install    # `go install` into $GOBIN (or $GOPATH/bin) with the same ldflags
make test       # pkg/bonsai{,/graphalgo}, pkg/graphql, pkg/audit, pkg/ui,
                #   cmd/bonsai/server, cmd/bonsai/tools
make vet        # vet bonsai-authored packages only (see note below)
make all        # vet + build + test
make clean      # rm bonsai

# Third-party API smoke (testdata/third-party-example imports pkg/bonsai
# as an external module — guards the v1 contract documented in pkg/bonsai/doc.go)
make smoke-as-third-party

# Run a single test
go test -count=1 -run TestDQLEdgeTraversal ./pkg/bonsai/...
go test -count=1 -run TestServerHTTP       ./cmd/bonsai/server/...

# Run the server (subcommand of the merged binary)
./bonsai server --dir ./data --http :8080 --grpc :9080
./bonsai server --dir ./data --trace-stdout                    # OTel stdout exporter
./bonsai server --tls-cert cert.pem --tls-key key.pem ...      # TLS on both HTTP+gRPC

# Client subcommands (talk to the gRPC server)
./bonsai alter '<schema>'
./bonsai query '<dql>'
./bonsai mutate '<rdf>'
./bonsai drop-all | drop-data

# Loaders
./bonsai bulk --dir ./data --rdfs goldendata.rdf
./bonsai live --addr 127.0.0.1:9080 --rdfs data.rdf

# One-shot "open this codebase in Bonsai": invokes gfy as a Go library
# (cmd/bonsai/tools/explore.go) to build the code-knowledge graph,
# ingests it, and starts the Explorer. The UI's Result panel has a Graph
# tab that renders any DQL result via vis-network.
./bonsai explore .                                             # current dir
./bonsai explore /path/to/repo --no-serve                      # ingest only
```

`make vet` deliberately scopes to `pkg/bonsai/...`, `cmd/bonsai/...`, `worker/...`.
`go vet ./...` reports many pre-existing `copylocks` warnings in proto-generated upstream
types — those are inherited and unrelated to bonsai work. Don't try to "fix" them.

## Architecture

```
cmd/bonsai/             single binary; main.go dispatches on os.Args[1] to:
cmd/bonsai/server/        HTTP + gRPC server. Routes: /query /mutate /alter /set /get
                          /assign /commit /abort /graphql /graphql/subscribe /ui
                          /admin/{backup,restore,export,import,schema,state,draining,
                          shutdown,namespace,config} /metrics /debug/pprof/*. The
                          gRPC adapter (grpc.go) implements api.DgraphServer so
                          existing dgo/v250 clients connect unchanged.
cmd/bonsai/cli/           Thin gRPC + HTTP client (alter/query/mutate/drop-* via
                          gRPC; backup/restore/export/import/download via HTTP).
cmd/bonsai/bulk/          Offline RDF/JSON ingest, opens the embedded DB directly.
cmd/bonsai/live/          Streaming RDF/JSON ingest over gRPC.
pkg/bonsai/             Go API: DB.{Open,Close,Alter,Mutate,Query,QueryWithVars,
                        QueryAsOf,Upsert,Set,Get,Backup,RestoreFrom,Export,
                        ExportTo,Drop{All,Data,Predicate,Type},SchemaText,
                        AssignUid,CreateNamespace,DropNamespace,ListNamespaces,
                        BackupTo,RestoreFromManifest,RestoreFromManifestWithOptions}.
                        db_test.go and feature_test.go are the e2e suite.
pkg/graphql/            GraphQL → DQL translator + WebSocket subscriptions.
pkg/audit/              JSON-lines audit log.
pkg/ui/                 Embedded Explorer (HTML+JS via go:embed).
worker/                 Ported from priorart with cluster forwarding paths excised.
                        mutation.go (runMutation, MutateOverNetwork), task.go (full
                        processTask — eq/ge/le/has/uid/sort/count/regex), sort.go,
                        match/compare/stringfilter/trigram/aggregator/tokens.
posting/ schema/ dql/   Core upstream packages, preserved and patched.
query/ types/ tok/
algo/ codec/ lex/
x/ protos/ task/
priorart/dgraph/        Gitignored reference copy of upstream. Read-only source for porting.
```

The dispatcher resets both stdlib `flag.CommandLine` and `pflag.CommandLine` before
calling each subcommand's `Main()`, so cli/bulk/live (stdlib `flag`) and server
(`pflag`) coexist in one binary without collision.

## Critical invariants

These are easy to break and hard to debug. Keep them in mind when touching the data path.

- **Single timestamp source**: `worker.localTs` (atomic, process-wide) is the only
  monotonic timestamp generator. `pkg/bonsai.DB.nextTs()` and `worker.NextTs()` both
  advance it. Don't add a second counter; the rewrite already removed the racy
  `nextLocalTs` loop that used to deadlock under load.

- **`posting.Oracle().ProcessDelta` is not concurrency-safe**: calling it from multiple
  paths trips an `AssertTrue` panic. Only the Open path seeds it; everything else relies
  on the seeded high-water mark. If you are tempted to call it elsewhere, don't.

- **No Raft, no group routing, no cluster forwarding**: when porting more code from
  `priorart/`, strip the `invokeNetworkRequest` / `processWithBackupRequest` / group
  sharding / `grpcWorker.ServeTask` paths. A process-wide lock serialises mutations.

- **UID counter persistence**: the high-water UID is stored under the reserved Badger
  key `__bonsai_max_uid`. On Open it's read into `worker.SetMaxUID`. The standard
  `x.DataKey/IndexKey/...` prefixes never collide with this key.

- **Cluster-only gRPC RPCs return `Unimplemented`** via `UnimplementedDgraphServer`
  embedding (Login, StreamExtSnapshot, etc.). Don't add fake implementations — dgo
  clients that don't call them work transparently. Namespace RPCs *are* implemented
  on `*DB` (single-node multi-tenancy via `x.NamespaceFromContext`).

- **Index rebuild requires `x.WorkerConfig.TmpDir`**: `Open` defaults it to `os.TempDir()`
  if blank. `posting.IndexRebuild` calls `os.MkdirTemp(TmpDir, ...)`; if you change Open,
  preserve this.

## Conventions

- Go module: `github.com/qiangli/bonsai`. Go 1.26.
- Badger lives at `<dir>/p` — never write outside it; reserved keys use a `__bonsai_`
  prefix to stay clear of upstream key prefixes.
- Schema: DQL syntax (`name: string @index(exact) . age: int .`). `Alter` snapshots the
  old schema, applies the new, and `posting.IndexRebuild.{DropIndexes,BuildIndexes}` runs
  when index directives change (see `TestAlterRebuildsIndex`).
- Backup format is single-file Badger Stream output. Upstream's multi-file manifest
  format is **not** produced — restore expects the single-file form.
- `REWRITE_STATUS.md` is the running ledger of what landed and what's still open from
  the cluster-removal rewrite. Consult it when porting more code from `priorart/`.
