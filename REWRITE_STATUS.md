# dgraph2 — Rewrite Status

This repo is mid-rewrite of upstream Dgraph (`priorart/dgraph/`) into a
lightweight, local-only graph database. The plan that drives the work lives at
`~/.claude/plans/plan-to-rewrite-priorart-dgraph-elegant-corbato.md`.

## Where we are

**Phase 0 — Copy and prune dead packages: DONE**
- Tree copied from `priorart/dgraph/` to repo root.
- Module renamed `github.com/dgraph-io/dgraph/v25` → `github.com/qiangli/dgraph2`.
- Deleted: `raftwal/`, `xidmap/`, `acl/`, `audit/`, `enc/`, `ocagent/`, `conn/`,
  `dgraph/cmd/{zero,cert,decrypt,dgraphimport,migrate,conv,mcp,debuginfo,increment}`,
  `dgraph/cmd/debug/wal.go`, ACL/multi-tenancy files in `worker/` and `edgraph/`,
  vault/JWT helpers in `x/`, ACL admin resolvers in `graphql/admin/`.

**Phase 1 — Stub cluster touchpoints: PARTIAL**
- `x/namespace_stub.go` replaces the deleted JWT helpers with always-namespace-0
  stubs (`ExtractNamespaceFrom`, `ParseJWT`, `MaybeKeyToBytes`).
- `worker/` reset to a single new `worker.go` (~150 LOC) that exposes the API
  surface `posting/` and `query/` import (`MutateOverNetwork`,
  `ProcessTaskOverNetwork`, `SortOverNetwork`, `AssignUidsOverNetwork`,
  `GetSchemaOverNetwork`, `GetTypes`, `Init`, `StartRaftNodes`, `MaxLeaseId`,
  `LimitDefaults`, `ErrNonExistentTabletMessage`).
- The "OverNetwork" entry points return `ErrNotImplemented` — they compile but
  do not yet execute queries or mutations against the posting store.
- Deleted heavy/coupled trees that we plan to rewrite from scratch later
  (rather than surgically strip): `edgraph/`, `graphql/`, `dgraph/cmd/{alpha,bulk,live,debug}`,
  `backup/`, `filestore/`, `testutil/`. The intent is to bring them back
  freshly and minimally as part of P2-P5.

**What compiles:**
```
go build ./x ./types ./tok ./algo ./codec ./dql ./lex ./schema ./worker ./task ./protos/...
```

**What does not yet compile:**
```
go build ./posting ./query ./chunker
```
Reason: `query/outputnode.go` and `query/outputnode_graphql.go` import
`graphql/schema` (deleted). Either restore a minimal stub of that package
(50+ methods on the `Field` interface) or split GraphQL output out of
`query/`. See "P1 follow-up" below.

## What is left

**P1 follow-up (current phase):**
1. Add a minimal `graphql/schema` package or refactor `query/outputnode_graphql.go`
   so `query/` compiles without the full GraphQL layer.
2. Port `worker/task.go` (~2,600 LOC upstream) for local query execution.
   This is the heart of `ProcessTaskOverNetwork` / `SortOverNetwork` and is
   currently a stub.
3. Wire `MutateOverNetwork` to a real local apply path. Upstream's
   `worker/embedded.go` had `ApplyMutations` calling into `node.applyMutations`
   in `draft.go`; we deleted both. Need to re-port the apply path without the
   Raft proposal step.
4. Persist `maxUID` to a Badger key (`__dgraph2_max_uid`) so it survives
   restart. Currently in-memory only.
5. Audit `posting.Oracle.WaitForTs` for deadlocks now that no external delta
   source advances `MaxAssigned`.

**P2 — Thin server (`cmd/dgraph2-server`):** not started.

**P3 — Library API (`pkg/dgraph2`):** not started.

**P4 — Single-node backup/restore/export:** not started; the upstream
`worker/backup.go`, `worker/restore_*.go`, and `worker/export.go` were deleted
and need to be re-ported with the per-group iteration removed.

**P5 — GraphQL admin trim:** not started; the entire `graphql/` tree is
currently absent.

**P6 — Build/test scaffolding + e2e smoke test:** not started.

## How to resume

1. Re-read the plan: `~/.claude/plans/plan-to-rewrite-priorart-dgraph-elegant-corbato.md`.
2. Start with the "P1 follow-up" item #1 — get `query/` compiling, since
   everything downstream depends on it.
3. Use `priorart/dgraph/` (gitignored) as the reference for the upstream
   implementations of any function being ported.
