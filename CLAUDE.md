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
make build      # go build ./cmd/bonsai-server
make test       # go test -count=1 ./pkg/bonsai/... ./cmd/bonsai-server/...
make vet        # go vet — only on bonsai-authored packages (see note below)
make all        # vet + build + test
make clean      # rm bonsai-server

# Run a single test
go test -count=1 -run TestDQLEdgeTraversal ./pkg/bonsai/...
go test -count=1 -run TestServerHTTP       ./cmd/bonsai-server/...

# Run the server
./bonsai-server --dir ./data --http :8080 --grpc :9080
./bonsai-server --dir ./data --trace-stdout                    # OTel stdout exporter
./bonsai-server --tls-cert cert.pem --tls-key key.pem ...      # TLS on both HTTP+gRPC

# CLI client (talks to the gRPC server)
./bonsai-cli alter '<schema>'
./bonsai-cli query '<dql>'
./bonsai-cli mutate '<rdf>'
./bonsai-cli drop-all | drop-data
```

`make vet` deliberately scopes to `pkg/bonsai/...`, `cmd/bonsai-server/...`, `worker/...`.
`go vet ./...` reports many pre-existing `copylocks` warnings in proto-generated upstream
types — those are inherited and unrelated to bonsai work. Don't try to "fix" them.

## Architecture

```
cmd/bonsai-server/    HTTP + gRPC server. HTTP routes: /query /mutate /alter /set /get
                       /assign /admin/{backup,restore,export,schema,state,draining,
                       shutdown,namespace} /metrics /debug/pprof/*. The gRPC adapter
                       (grpc.go) implements api.DgraphServer by delegating to *bonsai.DB,
                       so existing dgo/v250 clients connect unchanged.
cmd/bonsai-cli/       Thin gRPC client. Stateless, reconnects per invocation.
pkg/bonsai/           The Go API: DB.{Open,Close,Alter,Mutate,Query,QueryWithVars,
                       Upsert,Set,Get,Backup,RestoreFrom,Export,Drop{All,Data,Predicate,
                       Type},SchemaText,AssignUid,CreateNamespace,DropNamespace,
                       ListNamespaces}. db_test.go and feature_test.go are the e2e suite.
worker/                Ported from priorart with cluster forwarding paths excised.
                       mutation.go (runMutation, MutateOverNetwork), task.go (full
                       processTask — eq/ge/le/has/uid/sort/count/regex), sort.go,
                       match/compare/stringfilter/trigram/aggregator/tokens.
posting/ schema/ dql/  Core upstream packages, preserved and patched.
query/ types/ tok/
algo/ codec/ lex/
x/ protos/ task/
priorart/dgraph/       Gitignored reference copy of upstream. Read-only source for porting.
```

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
