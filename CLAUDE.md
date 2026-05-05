# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

`dgraph2` is a single-node, embeddable fork of upstream Dgraph. The cluster machinery
(Zero, Raft, inter-alpha gRPC, group sharding, distributed Oracle, ACL, at-rest encryption,
Vault) has been removed. The DQL parser, posting store, schema, indexing, and the worker
query-execution engine are preserved. A reference copy of upstream Dgraph lives at
`priorart/dgraph/` (gitignored) and is the source for porting work — read it, do not edit it.

`REWRITE_STATUS.md` is the running ledger of what has landed vs. what is still open
(bulk loader, GraphQL, upstream test fixtures). Skim it before starting a new feature.

## Build / test / run

```
make build      # go build ./cmd/dgraph2-server
make test       # go test -count=1 ./pkg/dgraph2/... ./cmd/dgraph2-server/...
make vet        # go vet — only on dgraph2-authored packages (see note below)
make all        # vet + build + test
make clean      # rm dgraph2-server

# Run a single test
go test -count=1 -run TestDQLEdgeTraversal ./pkg/dgraph2/...
go test -count=1 -run TestServerHTTP       ./cmd/dgraph2-server/...

# Run the server
./dgraph2-server --dir ./data --http :8080 --grpc :9080
./dgraph2-server --dir ./data --trace-stdout                    # OTel stdout exporter
./dgraph2-server --tls-cert cert.pem --tls-key key.pem ...      # TLS on both HTTP+gRPC

# CLI client (talks to the gRPC server)
./dgraph2-cli alter '<schema>'
./dgraph2-cli query '<dql>'
./dgraph2-cli mutate '<rdf>'
./dgraph2-cli drop-all | drop-data
```

`make vet` deliberately scopes to `pkg/dgraph2/...`, `cmd/dgraph2-server/...`, `worker/...`.
`go vet ./...` reports many pre-existing `copylocks` warnings in proto-generated upstream
types — those are inherited and unrelated to dgraph2 work. Don't try to "fix" them.

## Architecture

```
cmd/dgraph2-server/    HTTP + gRPC server. HTTP routes: /query /mutate /alter /set /get
                       /assign /admin/{backup,restore,export,schema,state,draining,
                       shutdown,namespace} /metrics /debug/pprof/*. The gRPC adapter
                       (grpc.go) implements api.DgraphServer by delegating to *dgraph2.DB,
                       so existing dgo/v250 clients connect unchanged.
cmd/dgraph2-cli/       Thin gRPC client. Stateless, reconnects per invocation.
pkg/dgraph2/           The Go API: DB.{Open,Close,Alter,Mutate,Query,QueryWithVars,
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
  monotonic timestamp generator. `pkg/dgraph2.DB.nextTs()` and `worker.NextTs()` both
  advance it. Don't add a second counter; the rewrite already removed the racy
  `nextLocalTs` loop that used to deadlock under load.

- **`posting.Oracle().ProcessDelta` is not concurrency-safe**: calling it from multiple
  paths trips an `AssertTrue` panic. Only the Open path seeds it; everything else relies
  on the seeded high-water mark. If you are tempted to call it elsewhere, don't.

- **No Raft, no group routing, no cluster forwarding**: when porting more code from
  `priorart/`, strip the `invokeNetworkRequest` / `processWithBackupRequest` / group
  sharding / `grpcWorker.ServeTask` paths. A process-wide lock serialises mutations.

- **UID counter persistence**: the high-water UID is stored under the reserved Badger
  key `__dgraph2_max_uid`. On Open it's read into `worker.SetMaxUID`. The standard
  `x.DataKey/IndexKey/...` prefixes never collide with this key.

- **Cluster-only gRPC RPCs return `Unimplemented`** via `UnimplementedDgraphServer`
  embedding (Login, StreamExtSnapshot, etc.). Don't add fake implementations — dgo
  clients that don't call them work transparently. Namespace RPCs *are* implemented
  on `*DB` (single-node multi-tenancy via `x.NamespaceFromContext`).

- **Index rebuild requires `x.WorkerConfig.TmpDir`**: `Open` defaults it to `os.TempDir()`
  if blank. `posting.IndexRebuild` calls `os.MkdirTemp(TmpDir, ...)`; if you change Open,
  preserve this.

## Conventions

- Go module: `github.com/qiangli/dgraph2`. Go 1.26.
- Badger lives at `<dir>/p` — never write outside it; reserved keys use a `__dgraph2_`
  prefix to stay clear of upstream key prefixes.
- Schema: DQL syntax (`name: string @index(exact) . age: int .`). `Alter` snapshots the
  old schema, applies the new, and `posting.IndexRebuild.{DropIndexes,BuildIndexes}` runs
  when index directives change (see `TestAlterRebuildsIndex`).
- Backup format is single-file Badger Stream output. Upstream's multi-file manifest
  format is **not** produced — restore expects the single-file form.
