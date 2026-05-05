# dgraph2 — Working Lightweight Graph Database

`dgraph2` is a single-node, embeddable fork of upstream Dgraph
(`priorart/dgraph/`, gitignored) with the cluster machinery (Zero, Raft,
inter-alpha gRPC, group sharding, distributed Oracle, ACL, multi-tenancy,
at-rest encryption, Vault) removed. The DQL parser, posting store, schema,
indexing and worker query-execution engine are all preserved.

## What works today

```
$ make all
go vet ./pkg/dgraph2/... ./cmd/dgraph2-server/... ./worker/...
go build  ./cmd/dgraph2-server
go test  -count=1 ./pkg/dgraph2/... ./cmd/dgraph2-server/...
ok    github.com/qiangli/dgraph2/pkg/dgraph2          2.570s
ok    github.com/qiangli/dgraph2/cmd/dgraph2-server   0.765s
```

`go build ./...` is clean across the whole tree.

### End-to-end capabilities

The following all work, demonstrated by e2e tests in
`pkg/dgraph2/db_test.go` and `cmd/dgraph2-server/e2e_test.go`:

| Feature                        | Test                          | Detail                                        |
|-------------------------------|------------------------------|----------------------------------------------|
| Open / Close                   | `TestOpenClose`               | Clean lifecycle, idempotent Close            |
| Schema persistence             | `TestAlterAndReopen`          | Alter survives Close + Open                  |
| Set / Get scalar triples       | `TestSetGetRoundtrip`         | Direct posting writes                        |
| Persistent timestamp + UID     | `TestPersistsAcrossReopen`    | Counters resumed from `pstore.MaxVersion`    |
| RDF batch ingest               | `TestMutateRDF`               | `_:blank` substitution → fresh UIDs           |
| DQL `eq()` over indexed pred   | `TestDQLQuery`                | `{"q":[{"uid":"0x1","name":"Alice","age":30}]}` |
| DQL `ge()`, `has()`            | `TestDQLFilters`              | Inequality + presence filters                |
| Pagination                     | `TestDQLPagination`           | `first: N` at root                           |
| RDF delete                     | `TestDQLDeleteRoundtrip`      | DelNquads round-trips                        |
| Multi-hop traversal            | `TestDQLEdgeTraversal`        | `friend { name }` expands edges              |
| `count(predicate)`             | `TestDQLCount`                | `@count` directive + count() function        |
| Index rebuild on Alter         | `TestAlterRebuildsIndex`      | Add `@index` after data → query still works  |
| Full DQL persist               | `TestDQLPersistsAcrossReopen` | Mutate + Query survive restart               |
| Backup / restore (DQL)         | `TestDQLBackupRestoreRoundtrip`| Stream-based backup → fresh DB → query        |
| HTTP `/query`, `/mutate`       | `TestServerHTTP`              | curl-able HTTP surface                       |

### Architecture

```
.
├── Makefile                     # build / test / vet / all / clean
├── cmd/dgraph2-server/          # HTTP server: /query /mutate /alter /admin/backup
├── pkg/dgraph2/                 # Go API: Open Close Alter Mutate Query Set Get Backup RestoreFrom
├── worker/                      # ported from priorart, cluster paths stripped:
│                                #   mutation.go (runMutation, MutateOverNetwork)
│                                #   task.go     (processTask, ProcessTaskOverNetwork)
│                                #   sort.go     (SortOverNetwork)
│                                #   match,compare,stringfilter,trigram,aggregator,tokens
├── posting/  schema/  dql/  query/  types/  tok/  algo/  codec/  lex/  x/  protos/  task/
│                                # core upstream packages, kept and patched
└── priorart/dgraph/             # gitignored reference copy of upstream Dgraph
```

## How the rewrite landed

### Tier 1 — done

* **#5/#6 Persistent counters**: timestamps resume from
  `pstore.MaxVersion()`, UID counter resumes from a reserved
  `__dgraph2_max_uid` Badger key. Single atomic `worker.localTs` is the
  process-wide source of truth; both `pkg/dgraph2.DB.tsCount` and
  `worker.NextTs` advance it. Calling `posting.Oracle.ProcessDelta`
  concurrently triggers an `AssertTrue` panic, so we never call it from
  multiple paths.
* **#1 Real `MutateOverNetwork`**: ~230 LOC in `worker/mutation.go`
  ported from priorart. Walks `pb.Mutations.Edges`, type-checks via
  `ValidateAndConvert`, applies through `posting.Txn.AddMutationWithIndex`
  → `CommitToDisk`. No Raft proposal; a process-wide lock serialises.
* **#2 `ProcessTaskOverNetwork`**: full ~2,800 LOC `worker/task.go`
  ported. Cluster forwarding paths
  (`invokeNetworkRequest`/`processWithBackupRequest`/group routing /
  `grpcWorker.ServeTask`) excised. The local executor that powers
  `eq`/`ge`/`le`/`has`/`uid`/sort/count/regex is kept verbatim.
* **#3 `SortOverNetwork`**: trivial wrapper over `processSort`.
* **#4 RDF Mutate**: `pkg/dgraph2.DB.Mutate` parses
  SetNquads/DelNquads via `chunker.ParseRDFs`, substitutes blank nodes
  with fresh UIDs, tags Set/Del, routes through `MutateOverNetwork`.

### Tier 2 — done

* **#7 DQL Query**: `pkg/dgraph2.DB.Query` parses through `dql.Parse`,
  builds `query.Request`, runs `ProcessQuery`, marshals via
  `query.ToJson`. `cmd/dgraph2-server` exposes `/query` and `/mutate`.
* **#8 HTTP** (gRPC was originally planned via `api.DgraphServer`; the
  HTTP surface is the actual interface for now — the same gRPC types are
  reused on the wire so dgo clients can be wired later trivially).
* **#9 Indexing on Alter**: `Alter` snapshots the old schema, applies
  the new, and calls `posting.IndexRebuild.{DropIndexes,BuildIndexes}`
  when directives change. Verified by `TestAlterRebuildsIndex`.

### Tier 3 — partial

* **#11 Bulk + live loaders** — deferred. The upstream `dgraph/cmd/bulk`
  and `dgraph/cmd/live` were deleted in P0 because they imported the
  removed `xidmap` and `enc` packages. Bringing them back wired to
  `worker.AssignUidsOverNetwork` is straightforward but untouched.
* **#12 Backup format compat** — current backup is single-file
  Badger Stream output. The upstream multi-file manifest format is not
  produced.
* **#10 GraphQL** — deferred. The 60K+ LOC `graphql/` tree is gone.

### Tier 4 — partial

* **#13 WaitForTs deadlock audit** — fixed. The unified counter and the
  removal of the racy `nextLocalTs` loop closed the deadlock paths.
* **#14 Run upstream tests** — not done. `query/query{1,2}_test.go` and
  similar were left in but rely on cluster fixtures; they're still in
  the tree but not part of `make test`.
* **#15 Health / metrics** — `/health` works; Prometheus metrics
  collectors are linked but not exposed.

## Honest open work

If you keep going, in rough priority:

1. **Bulk loader** — port `dgraph/cmd/bulk` from priorart, swapping
   `xidmap` for `worker.AssignUidsOverNetwork`. Lets users ingest
   multi-GB RDF dumps without the streaming Mutate path.
2. **gRPC `api.DgraphServer`** — register the auto-generated server in
   `cmd/dgraph2-server`. Existing dgo clients connect unchanged.
3. **GraphQL** — restore the trimmed `graphql/` tree (admin endpoints
   without ACL/namespace SDL fragments). Largest remaining piece of
   upstream code that's currently absent.
4. **Run the kept upstream tests** — `posting/list_test.go`,
   `query/query{1,2}_test.go`, `schema/schema_test.go`. Most should
   pass with minor fixture surgery now that the engine is back.
5. **Schema GraphQL `@auth`** is currently a no-op since ACL is gone;
   if you bring auth back you will need to put the directive evaluator
   back too.
