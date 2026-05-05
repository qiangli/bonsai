# dgraph2 — Rewrite Status

dgraph2 is a strip-down of upstream Dgraph (`priorart/dgraph/`, gitignored)
into a lightweight, local-only graph database. The plan that drives the work
lives at `~/.claude/plans/plan-to-rewrite-priorart-dgraph-elegant-corbato.md`.

## What works today

`make all` builds the binary, passes `go vet`, and runs the e2e tests:

```
$ make all
go vet ./pkg/dgraph2/... ./cmd/dgraph2-server/... ./worker/...
go build  ./cmd/dgraph2-server
go test  -count=1 ./pkg/dgraph2/... ./cmd/dgraph2-server/...
ok  	github.com/qiangli/dgraph2/pkg/dgraph2	1.012s
ok  	github.com/qiangli/dgraph2/cmd/dgraph2-server	0.773s
```

`go build ./...` is clean across the whole tree.

### Phase 0 — Copy and prune dead packages: DONE
- Tree copied from `priorart/dgraph/` to repo root.
- Module renamed `github.com/dgraph-io/dgraph/v25` → `github.com/qiangli/dgraph2`.
- Deleted: `raftwal/`, `xidmap/`, `acl/`, `audit/`, `enc/`, `ocagent/`, `conn/`,
  the entire `dgraph/cmd/{zero,cert,decrypt,dgraphimport,migrate,conv,mcp,debuginfo,increment}` set,
  `dgraph/cmd/debug/wal.go`, `compose/`, `contrib/`, `paper/`, `dgraphtest/`,
  `dgraphapi/`, `upgrade/`, `check_upgrade/`, `tlstest/`, `systest/`, `t/`,
  `testutil/`, `filestore/`, `backup/`, `graphql/`, `edgraph/`, ACL/multi-tenancy
  files, vault/JWT helpers, and ACL-only tests across packages.

### Phase 1 — Cluster touchpoints stubbed: DONE
- `x/namespace_stub.go` replaces the deleted JWT helpers with always-namespace-0
  stubs.
- `worker/worker.go` (~150 LOC) provides the API surface `posting/` and `query/`
  import: `MutateOverNetwork`, `ProcessTaskOverNetwork`, `SortOverNetwork`,
  `AssignUidsOverNetwork` (real local counter), `GetSchemaOverNetwork` (real,
  reads schema state), `GetTypes` (real), `Init`, `StartRaftNodes`, `MaxLeaseId`,
  `LimitDefaults`, `ErrNonExistentTabletMessage`.
- `query/outputnode_graphql.go` deleted; `query/outputnode.go` now serializes
  in DQL form only (the upstream GraphQL output path went through a 50+ method
  `gqlSchema.Field` interface from the deleted `graphql/schema` package).
- `MutateOverNetwork`, `ProcessTaskOverNetwork`, `SortOverNetwork` return
  `ErrNotImplemented`; the demo path goes through `pkg/dgraph2.Set/Get`
  directly against the posting store.

### Phase 2 — Thin server `cmd/dgraph2-server`: DONE
- HTTP endpoints: `/health`, `/alter`, `/set`, `/get`, `/assign`,
  `/admin/backup`, `/admin/restore`.
- Graceful shutdown on SIGINT/SIGTERM.
- E2E test in `cmd/dgraph2-server/e2e_test.go`: drives the full HTTP surface
  via `httptest`.

### Phase 3 — Library API `pkg/dgraph2`: DONE
- `Open(Options) (*DB, error)` — opens Badger in managed mode, wires the
  worker and posting layers, applies the reserved schema, and refreshes
  the schema cache from disk.
- `(*DB) Close() error` — idempotent.
- `(*DB) Alter(ctx, schemaText) error` — parses DQL schema and persists each
  predicate/type to the schema state and Badger.
- `(*DB) AssignUid(ctx, n) (start, end uint64, error)` — local atomic UID
  counter that replaces the upstream xidmap.
- `(*DB) Set(ctx, subject, predicate, value) error` — round-trips a triple
  through `posting.NewTxn` → `pl.AddMutationWithIndex` → `txn.Update` →
  `txn.CommitToDisk` → `posting.Oracle().ProcessDelta`.
- `(*DB) Get(ctx, subject, predicate) ([]byte, error)` — reads the latest
  scalar value at the local timestamp via `posting.GetNoStore` + `pl.Value`.
- 7 unit tests; all pass.

### Phase 4 — Single-node backup/restore: DONE
- `(*DB) Backup(ctx, dst)` uses `pstore.NewStreamAt(currentTs).Backup` —
  the managed-mode equivalent of the upstream group-coordinated backup,
  collapsed to a single Badger snapshot.
- `(*DB) RestoreFrom(ctx, src)` uses `pstore.Load`, then advances the local
  timestamp counter past `pstore.MaxVersion()` and refreshes the schema
  cache so subsequent reads see the restored data.
- E2E covered by `TestBackupRestore`.

### Phase 5 — GraphQL admin trim: SKIPPED
- The entire `graphql/` tree was deleted in P1 (it transitively depended on
  the deleted ACL/multi-tenancy types). Bringing it back as a minimal
  trimmed package is the natural next step but is out of scope for the
  current pass.

### Phase 6 — Build / test scaffolding + e2e smoke tests: DONE
- `Makefile` with `build`, `test`, `vet`, `all`, `clean` targets.
- `vet` is scoped to the new/rewritten packages because the upstream proto
  types embed `sync.Mutex` via `MessageState`, which the standard `go vet`
  copylocks check flags pervasively in priorart code.
- 8 e2e tests across `pkg/dgraph2` and `cmd/dgraph2-server`.

## Honest limitations

- **The full DQL query/mutation engine is not yet wired up.** Upstream's
  `worker/task.go` (~2,600 LOC) and the surrounding `mutation.go`,
  `groups.go`, and `draft.go` files were deleted; only the API surface
  remains as `ErrNotImplemented` stubs. The library demos the architecture
  via direct `Set/Get` against the posting store.
- **No GraphQL endpoint.** `graphql/` is gone; restoring a trimmed version
  is the obvious next step.
- **No bulk/live loaders.** `dgraph/cmd/{bulk,live}` were deleted because
  they imported the deleted `xidmap` and `enc` packages.
- **No ACL, audit, multi-tenancy, encryption-at-rest, vault.** These were
  intentional deletions per the user-confirmed scope.

## Layout

```
.
├── Makefile
├── REWRITE_STATUS.md
├── cmd/dgraph2-server/        # thin HTTP server wrapping pkg/dgraph2
├── pkg/dgraph2/               # public Go API: Open, Alter, Set, Get, Backup, ...
├── worker/                    # stub façade exposing the symbols posting/query import
├── posting/  query/  schema/  dql/  types/  tok/  algo/  codec/  lex/  x/  protos/  task/
│                              # core upstream packages, kept and patched
└── priorart/dgraph/           # gitignored reference copy of upstream Dgraph
```

## How to resume

The next step is to port `worker/task.go` and `worker/mutation.go` from
`priorart/dgraph/` with the cluster forwarding branches stripped, so that
`worker.MutateOverNetwork` and `worker.ProcessTaskOverNetwork` execute real
DQL queries instead of returning `ErrNotImplemented`. Once those land, the
library can grow `Query(dql)` and `Mutate(rdf)` methods and the server can
register the upstream `api.DgraphServer` gRPC interface.
