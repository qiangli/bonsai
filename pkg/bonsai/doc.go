/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// Package bonsai is the public, semver-stable Go API for the bonsai
// embedded graph database.
//
// # Stability
//
// This package follows semver. Anything exported here is part of the v1
// surface and won't change without a major-version bump:
//
//   - Open(opts Options) (*DB, error)
//   - OpenFrozen(path string) (*DB, error)
//   - Freeze(srcDir, dstFile string) error
//   - (*DB).Close() error
//   - (*DB).ReadOnly() bool
//   - (*DB).MutationTick() uint64
//   - (*DB).NextReadableTs() uint64
//
//   - (*DB).Alter, Mutate, Upsert, Set, Get
//   - (*DB).Query, QueryWithVars, QueryAsOf, QueryWithVarsAsOf
//   - (*DB).Backup, BackupTo, RestoreFrom, RestoreFromManifest,
//     RestoreFromManifestWithOptions
//   - (*DB).Export, ExportTo
//   - (*DB).Drop{All,Data,Predicate,Type}, SchemaText
//   - (*DB).AssignUid, MaxUid
//   - (*DB).CreateNamespace, DropNamespace, ListNamespaces
//
//   - Options, BackupOptions, RestoreOptions, BackupType, BackupFull,
//     BackupIncremental, ImportSummary, Manifest, MasterManifest
//   - ErrReadOnly, ErrNoValue
//   - ImportStream
//
// And the graphalgo helpers — see pkg/bonsai/graphalgo/graphalgo.go.
// They are part of v1.
//
// Anything in pkg/audit/, pkg/graphql/, pkg/ui/, or cmd/bonsai/* is
// experimental — useful, but the API may change between minor versions.
// Anything in worker/, posting/, schema/, query/, dql/, types/, tok/,
// algo/, codec/, lex/, x/, protos/, task/, chunker/ is internal:
// inherited from upstream Dgraph and not part of the bonsai contract.
//
// # Embedded use
//
//	import "github.com/qiangli/bonsai/pkg/bonsai"
//
//	db, _ := bonsai.Open(bonsai.Options{Dir: "./data"})
//	defer db.Close()
//
//	db.Alter(ctx, "name: string @index(exact) .")
//	db.Mutate(ctx, &api.Mutation{
//	    SetNquads: []byte(`_:a <name> "Alice" .`),
//	})
//	resp, _ := db.Query(ctx, `{ q(func: eq(name, "Alice")) { name } }`)
//
// For build-once / query-many workflows, freeze a single-file artifact
// and ship it:
//
//	bonsai.Freeze("./data", "./graph.bonsai")
//	// later, possibly in a different process:
//	db, _ := bonsai.OpenFrozen("./graph.bonsai")
//	defer db.Close() // also removes the extracted temp dir
//
// # Migration paths
//
// Out: bonsai.BackupTo produces upstream-compatible backup directories
// (manifest.json + r<ReadTs>-g1.backup files) that upstream Dgraph's
// `dgraph restore` accepts. Format compliance is asserted by
// TestBackupManifestUpstreamFormat.
//
// In: ImportStream auto-detects NetworkX node-link and Cytoscape
// elements JSON in addition to Dgraph-flavored RDF/JSON.
package bonsai
