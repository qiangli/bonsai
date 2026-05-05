# Bonsai

A single-node, embeddable graph database. Bonsai is a carefully pruned fork of
[Dgraph](https://github.com/hypermodeinc/dgraph) — same DQL, same posting store,
same indexer, same query engine, with the cluster machinery (Zero, Raft,
inter-alpha gRPC, group sharding, distributed Oracle, ACL, encryption-at-rest,
Vault) removed.

The metaphor: a bonsai is the same plant as the full-size tree, trimmed to a
manageable form while keeping the structure intact.

## Quick start

```sh
make build               # builds the single `bonsai` binary
./bonsai server --dir ./data
```

The server listens on `:8080` (HTTP) and `:9080` (gRPC) by default. Open
[http://localhost:8080/](http://localhost:8080/) for the bundled Explorer UI.

All client and loader operations are subcommands of the same binary:

```sh
./bonsai alter 'name: string @index(exact) .'
./bonsai mutate '_:a <name> "Alice" .'
./bonsai query '{ q(func: eq(name, "Alice")) { uid name } }'

./bonsai bulk --dir ./data --rdfs goldendata.rdf      # offline batch loader
./bonsai live --addr 127.0.0.1:9080 --rdfs data.rdf   # streaming loader
./bonsai backup ./snapshot.bk                         # single-file backup
./bonsai backup manifest ./backups                    # multi-file manifest
./bonsai import json ./graph.json                     # auto-detects shape
./bonsai download rdf > dump.rdf                      # streaming export
./bonsai version                                      # build metadata
```

Run `bonsai help` for the full subcommand list, `bonsai <subcommand> --help`
for subcommand-specific flags.

Use as a Go library:

```go
import "github.com/qiangli/bonsai/pkg/bonsai"

db, _ := bonsai.Open(bonsai.Options{Dir: "./data"})
defer db.Close()
db.Alter(ctx, "name: string @index(exact) .")
db.Mutate(ctx, &api.Mutation{SetNquads: []byte(`_:a <name> "Alice" .`)})
resp, _ := db.Query(ctx, `{ q(func: eq(name, "Alice")) { name } }`)
```

## What's included

- **DQL** — full upstream parser and engine: `eq`, `ge`, `le`, `has`,
  `anyofterms`, `alloftext`, `regex`, `match`, `between`, `near`, sort,
  pagination, `count`, aggregations, variables, upsert blocks, `@cascade`,
  `@recurse`, `@groupby`, `@facets`, `@lang`, shortest path.
- **GraphQL** — `/graphql` endpoint with `query<T>`, `get<T>(id)`, nested
  field expansion, and `add<T>` / `update<T>` / `delete<T>` mutations.
  WebSocket subscriptions at `/graphql/subscribe` (graphql-transport-ws
  protocol, mutation-tick driven push).
- **HTTP** — `/query`, `/mutate`, `/alter`, `/commit`, `/abort`,
  `/admin/{backup,restore,export,import,schema,state,draining,shutdown,namespace,config}`,
  `/metrics`, `/debug/pprof/*`.
- **gRPC** — `api.DgraphServer` adapter; existing dgo clients connect
  unchanged.
- **Multi-tenancy** — namespaces with isolation (no ACL).
- **Loaders** — `bonsai bulk` (offline embedded ingest) and `bonsai live`
  (gRPC streaming ingest) for RDF and JSON.
- **Backup / restore** — single-file Badger Stream and upstream-compatible
  multi-file manifest format with full + incremental chains.
- **Observability** — Prometheus `/metrics`, OpenTelemetry exporters
  (stdout, OTLP/HTTP, OTLP/gRPC, Jaeger preset), pprof, JSON-lines audit
  log.
- **Operational** — TLS for HTTP + gRPC, graceful shutdown, draining
  mode, env / YAML / flag config, build-time version.
- **Embedded UI** — single-page Explorer at `/` with schema sidebar,
  quick filter, and DQL editor. No build step.

## What's deliberately not included

- Cluster machinery: Zero, Raft, group sharding, inter-alpha gRPC,
  distributed Oracle, snapshot streaming.
- ACL / JWT / `@auth` / encryption-at-rest / Vault — enterprise features
  in upstream Dgraph; out of scope for a single-node fork.
- GraphQL SDL upload, `@custom`, `@lambda`, schema introspection.
- S3 / Minio backup targets (local filesystem only).

`REWRITE_STATUS.md` is the running ledger of what landed and what's open.

## Architecture (one screen)

```
cmd/bonsai/          top-level dispatcher; one binary, subcommands route into:
cmd/bonsai/server/     HTTP + gRPC server
cmd/bonsai/cli/        gRPC + HTTP admin client (alter/query/mutate/...)
cmd/bonsai/bulk/       offline RDF/JSON ingest, opens DB directly
cmd/bonsai/live/       streaming RDF/JSON ingest over gRPC
pkg/bonsai/          Go API: Open / Close / Alter / Mutate / Query / ...
pkg/graphql/         GraphQL → DQL translator + WebSocket subscriptions
pkg/audit/           JSON-lines audit log
pkg/ui/              embedded Explorer (HTML+JS via go:embed)
worker/              ported from upstream, cluster paths excised
posting/ schema/ dql/ query/ types/ tok/ algo/ codec/ lex/ x/ protos/ task/
                     core upstream packages, preserved and patched
priorart/dgraph/     read-only reference copy of upstream Dgraph (gitignored)
```

## Building and testing

```sh
make build     # single `bonsai` binary
make test      # ./pkg/bonsai/... ./pkg/graphql/... ./pkg/audit/... ./pkg/ui/... ./cmd/bonsai/server/...
make vet       # bonsai-authored packages only (skips inherited copylocks warnings)
make all       # vet + build + test
```

Single test:

```sh
go test -count=1 -run TestDQLEdgeTraversal ./pkg/bonsai/...
```

## Credits

Bonsai stands on the shoulders of [Dgraph](https://github.com/hypermodeinc/dgraph)
by Hypermode (formerly Dgraph Labs). The DQL parser, posting store, schema state,
indexing pipeline, query engine, tokenizers, and most of the supporting
machinery are upstream Dgraph code carried into this fork — see `priorart/dgraph/`
for the read-only reference copy used during the trimming work. Everything good
about graph storage and query evaluation here is theirs; the bugs introduced by
removing the cluster layer are ours.

Other significant dependencies, all shared with upstream:

- [Badger](https://github.com/dgraph-io/badger) — embedded key-value store
- [dgo](https://github.com/dgraph-io/dgo) — Go client and protobuf API
- [gqlparser](https://github.com/dgraph-io/gqlparser) — GraphQL parser
- [gorilla/websocket](https://github.com/gorilla/websocket) — subscription transport
- [klauspost/compress](https://github.com/klauspost/compress) — snappy backup framing
- [OpenTelemetry-Go](https://github.com/open-telemetry/opentelemetry-go)

## License

Apache License 2.0. See [LICENSE.txt](LICENSE.txt). Files inherited from
upstream Dgraph retain their original copyright headers; new files carry an
`SPDX-FileCopyrightText: bonsai contributors` line.
