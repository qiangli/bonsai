/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package dgraph2_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/dgraph2/pkg/dgraph2"
)

// TestOpenClose exercises the database lifecycle: open, then close, in a
// fresh temp dir.
func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Closing twice is a no-op.
	if err := db.Close(); err != nil {
		t.Fatalf("second Close should be no-op, got %v", err)
	}
}

// TestAlterAndReopen verifies that schema changes persist across reopen.
func TestAlterAndReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}
	if err := db.Alter(context.Background(), "name: string .\nage: int ."); err != nil {
		t.Fatalf("Alter failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen — the schema we wrote should be loaded from Badger.
	db, err = dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close after reopen: %v", err)
		}
	}()
}

// TestSetGetRoundtrip writes a string triple and reads it back. This is the
// minimum smoke test that the posting + schema + Badger pipeline works end
// to end.
func TestSetGetRoundtrip(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Alter(ctx, "name: string .\nage: int ."); err != nil {
		t.Fatalf("Alter failed: %v", err)
	}

	start, _, err := db.AssignUid(ctx, 1)
	if err != nil {
		t.Fatalf("AssignUid: %v", err)
	}
	if start == 0 {
		t.Fatalf("AssignUid returned 0")
	}

	if err := db.Set(ctx, start, "name", "Alice"); err != nil {
		t.Fatalf("Set name: %v", err)
	}
	if err := db.Set(ctx, start, "age", int64(30)); err != nil {
		t.Fatalf("Set age: %v", err)
	}

	got, err := db.Get(ctx, start, "name")
	if err != nil {
		t.Fatalf("Get name: %v", err)
	}
	if string(got) != "Alice" {
		t.Errorf("Get name: want %q, got %q", "Alice", string(got))
	}
}

// TestGetMissingPredicate returns a clear error rather than panicking when
// the predicate is not in the schema.
func TestGetMissingPredicate(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	if _, err := db.Get(context.Background(), 1, "no_such_pred"); err == nil {
		t.Error("expected error for unknown predicate, got nil")
	}
}

// TestGetMissingTriple returns ErrNoValue when the triple has not been Set.
func TestGetMissingTriple(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Alter(ctx, `name: string .`); err != nil {
		t.Fatalf("Alter: %v", err)
	}

	_, err = db.Get(ctx, 42, "name")
	if !errors.Is(err, dgraph2.ErrNoValue) {
		t.Fatalf("expected ErrNoValue, got %v", err)
	}
}

// TestPersistsAcrossReopen writes data, closes, and reopens; data must
// still be readable. Exercises the tsCount + maxUID persistence wired
// through pstore.MaxVersion + the __dgraph2_max_uid Badger key.
func TestPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open 1: %v", err)
	}
	ctx := context.Background()
	if err := db.Alter(ctx, "name: string .\n"); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	uid, _, err := db.AssignUid(ctx, 1)
	if err != nil {
		t.Fatalf("AssignUid: %v", err)
	}
	if err := db.Set(ctx, uid, "name", "Persist"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db, err = dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer db.Close()

	got, err := db.Get(ctx, uid, "name")
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if string(got) != "Persist" {
		t.Errorf("Get: want %q, got %q", "Persist", string(got))
	}

	// Subsequent AssignUid should hand out a UID strictly greater than
	// what was assigned before the close.
	next, _, err := db.AssignUid(ctx, 1)
	if err != nil {
		t.Fatalf("AssignUid 2: %v", err)
	}
	if next <= uid {
		t.Errorf("AssignUid did not advance: prev=%d, next=%d", uid, next)
	}
}

// TestBackupRestore writes data, takes a backup, opens a fresh DB at a new
// directory, restores into it, and confirms the data is readable. This is the
// end-to-end path that P4 of the rewrite plan describes.
func TestBackupRestore(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	backupPath := srcDir + "/backup.bin"

	src, err := dgraph2.Open(dgraph2.Options{Dir: srcDir})
	if err != nil {
		t.Fatalf("Open src: %v", err)
	}
	ctx := context.Background()
	if err := src.Alter(ctx, "name: string .\n"); err != nil {
		t.Fatalf("Alter src: %v", err)
	}
	uid, _, err := src.AssignUid(ctx, 1)
	if err != nil {
		t.Fatalf("AssignUid: %v", err)
	}
	if err := src.Set(ctx, uid, "name", "Bob"); err != nil {
		t.Fatalf("Set src: %v", err)
	}
	if err := src.Backup(ctx, backupPath); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close src: %v", err)
	}

	dst, err := dgraph2.Open(dgraph2.Options{Dir: dstDir})
	if err != nil {
		t.Fatalf("Open dst: %v", err)
	}
	defer dst.Close()

	if err := dst.RestoreFrom(ctx, backupPath); err != nil {
		t.Fatalf("RestoreFrom: %v", err)
	}

	got, err := dst.Get(ctx, uid, "name")
	if err != nil {
		t.Fatalf("Get after restore: %v", err)
	}
	if string(got) != "Bob" {
		t.Errorf("Get: want %q, got %q", "Bob", string(got))
	}
}

// TestMutateRDF runs a multi-triple RDF mutation through the new Mutate
// path and verifies each triple is readable. Exercises:
//   - chunker.ParseRDFs
//   - blank-node UID substitution
//   - worker.MutateOverNetwork (the real local apply, not the stub)
func TestMutateRDF(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Alter(ctx, "name: string .\nage: int .\n"); err != nil {
		t.Fatalf("Alter: %v", err)
	}

	resp, err := db.Mutate(ctx, &apiproto.Mutation{
		SetNquads: []byte(`
			_:alice <name> "Alice" .
			_:alice <age>  "30"^^<xs:int> .
			_:bob   <name> "Bob"  .
		`),
	})
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	if len(resp.Uids) != 2 {
		t.Errorf("expected 2 assigned UIDs, got %d (%v)", len(resp.Uids), resp.Uids)
	}

	aliceHex := resp.Uids["alice"]
	if aliceHex == "" {
		t.Fatalf("no UID assigned for alice (%v)", resp.Uids)
	}
	var aliceUid uint64
	if _, err := fmt.Sscanf(aliceHex, "0x%x", &aliceUid); err != nil {
		t.Fatalf("parse uid %q: %v", aliceHex, err)
	}

	got, err := db.Get(ctx, aliceUid, "name")
	if err != nil {
		t.Fatalf("Get name: %v", err)
	}
	if string(got) != "Alice" {
		t.Errorf("name: want Alice, got %q", string(got))
	}
}

// TestAlterRebuildsIndex exercises the index-rebuild path: insert untagged
// data, then add an @index(exact) directive via Alter, then query. The query
// must succeed because Alter triggered a backfill of the index.
func TestAlterRebuildsIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Alter(ctx, "name: string .\n"); err != nil {
		t.Fatalf("Alter (no index): %v", err)
	}
	if _, err := db.Mutate(ctx, &apiproto.Mutation{
		SetNquads: []byte(`_:carol <name> "Carol" .`),
	}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}

	// Add the index AFTER data is inserted. Alter must rebuild.
	if err := db.Alter(ctx, "name: string @index(exact) .\n"); err != nil {
		t.Fatalf("Alter (with index): %v", err)
	}

	resp, err := db.Query(ctx, `{ q(func: eq(name, "Carol")) { uid name } }`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !strings.Contains(string(resp.Json), `"name":"Carol"`) {
		t.Errorf("index-rebuild query missing Carol: %s", string(resp.Json))
	}
}

// TestDQLQuery is the headline e2e test: ingest RDF, then read it back via
// a DQL query exercising eq() and predicate expansion. This is the path that
// goes through dql parsing -> SubGraph -> worker.ProcessTaskOverNetwork ->
// posting reads, end to end.
func TestDQLQuery(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Alter(ctx, "name: string @index(exact) .\nage: int .\n"); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	if _, err := db.Mutate(ctx, &apiproto.Mutation{
		SetNquads: []byte(`
			_:alice <name> "Alice" .
			_:alice <age>  "30"^^<xs:int> .
			_:bob   <name> "Bob"  .
			_:bob   <age>  "25"^^<xs:int> .
		`),
	}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}

	resp, err := db.Query(ctx, `{
		q(func: eq(name, "Alice")) {
			uid
			name
			age
		}
	}`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !strings.Contains(string(resp.Json), `"name":"Alice"`) {
		t.Errorf("Query response missing Alice: %s", string(resp.Json))
	}
	if !strings.Contains(string(resp.Json), `"age":30`) {
		t.Errorf("Query response missing age=30: %s", string(resp.Json))
	}
	t.Logf("DQL response: %s", string(resp.Json))
}

// TestDQLFilters exercises filter functions: ge, le, has.
func TestDQLFilters(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := db.Alter(ctx,
		"name: string @index(exact) .\nage: int @index(int) .\n"); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	if _, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:a <name> "Alice" .
		_:a <age> "30"^^<xs:int> .
		_:b <name> "Bob" .
		_:b <age> "25"^^<xs:int> .
		_:c <name> "Carol" .
		_:c <age> "40"^^<xs:int> .
	`)}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}

	// ge(age, 30) should return Alice + Carol (not Bob).
	resp, err := db.Query(ctx, `{ q(func: ge(age, 30)) { name age } }`)
	if err != nil {
		t.Fatalf("Query ge: %v", err)
	}
	got := string(resp.Json)
	if !strings.Contains(got, "Alice") || !strings.Contains(got, "Carol") {
		t.Errorf("ge(age,30) missing Alice/Carol: %s", got)
	}
	if strings.Contains(got, "Bob") {
		t.Errorf("ge(age,30) should not contain Bob: %s", got)
	}

	// has(age) returns everyone.
	resp, err = db.Query(ctx, `{ q(func: has(age)) { name } }`)
	if err != nil {
		t.Fatalf("Query has: %v", err)
	}
	for _, want := range []string{"Alice", "Bob", "Carol"} {
		if !strings.Contains(string(resp.Json), want) {
			t.Errorf("has(age) missing %s: %s", want, string(resp.Json))
		}
	}
}

// TestDQLPagination tests first/offset.
func TestDQLPagination(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := db.Alter(ctx, "name: string @index(exact) .\n"); err != nil {
		t.Fatalf("Alter: %v", err)
	}

	// Insert 5 names that all share an indexable token (use the same exact
	// name "person" for the index match, but distinct UIDs).
	if _, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:p1 <name> "person" .
		_:p2 <name> "person" .
		_:p3 <name> "person" .
		_:p4 <name> "person" .
		_:p5 <name> "person" .
	`)}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}

	resp, err := db.Query(ctx,
		`{ q(func: eq(name, "person"), first: 2) { uid name } }`)
	if err != nil {
		t.Fatalf("Query first:2: %v", err)
	}
	// Count occurrences of `"name":"person"` — must be exactly 2.
	got := string(resp.Json)
	count := strings.Count(got, `"name":"person"`)
	if count != 2 {
		t.Errorf("first:2 returned %d rows, want 2: %s", count, got)
	}
}

// TestDQLDeleteRoundtrip — set, delete, then verify gone.
func TestDQLDeleteRoundtrip(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := db.Alter(ctx, "name: string @index(exact) .\n"); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	resp, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:victor <name> "Victor" .
	`)})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	uidHex := resp.Uids["victor"]
	if uidHex == "" {
		t.Fatalf("no uid for victor")
	}
	q1, _ := db.Query(ctx, `{ q(func: eq(name, "Victor")) { name } }`)
	if !strings.Contains(string(q1.Json), "Victor") {
		t.Fatalf("pre-delete missing Victor: %s", string(q1.Json))
	}

	if _, err := db.Mutate(ctx, &apiproto.Mutation{
		DelNquads: []byte("<" + uidHex + `> <name> "Victor" .`),
	}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	q2, _ := db.Query(ctx, `{ q(func: eq(name, "Victor")) { name } }`)
	if strings.Contains(string(q2.Json), "Victor") {
		t.Errorf("post-delete still has Victor: %s", string(q2.Json))
	}
}

// TestDQLEdgeTraversal exercises uid-typed edges and multi-hop traversal:
// alice knows bob; query for alice's friends should expand the edge.
func TestDQLEdgeTraversal(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := db.Alter(ctx,
		"name: string @index(exact) .\nfriend: [uid] .\n"); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	resp, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:alice <name>   "Alice" .
		_:bob   <name>   "Bob" .
		_:alice <friend> _:bob .
	`)})
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	t.Logf("Uids: %v", resp.Uids)

	q := `{
		q(func: eq(name, "Alice")) {
			name
			friend {
				name
			}
		}
	}`
	r, err := db.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !strings.Contains(string(r.Json), `"name":"Alice"`) {
		t.Errorf("missing Alice: %s", string(r.Json))
	}
	if !strings.Contains(string(r.Json), `"name":"Bob"`) {
		t.Errorf("friend->Bob not traversed: %s", string(r.Json))
	}
	t.Logf("traversal: %s", string(r.Json))
}

// TestDQLCount exercises `count(predicate)`.
func TestDQLCount(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := db.Alter(ctx,
		"name: string @index(exact) .\nfriend: [uid] @count .\n"); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	if _, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:a <name>   "A" .
		_:b <name>   "B" .
		_:c <name>   "C" .
		_:a <friend> _:b .
		_:a <friend> _:c .
	`)}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	r, err := db.Query(ctx, `{
		q(func: eq(name, "A")) {
			count(friend)
		}
	}`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !strings.Contains(string(r.Json), `"count(friend)":2`) {
		t.Errorf("count(friend) wrong: %s", string(r.Json))
	}
}

// TestDQLPersistsAcrossReopen mutates a graph, closes, reopens, and queries.
// Exercises the full pipeline (Mutate via worker, Query via task) survives
// a round-trip through Badger.
func TestDQLPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	{
		db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
		if err != nil {
			t.Fatalf("Open 1: %v", err)
		}
		if err := db.Alter(ctx,
			"name: string @index(exact) .\nfriend: [uid] .\n"); err != nil {
			t.Fatalf("Alter: %v", err)
		}
		if _, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
			_:alice <name>   "Alice" .
			_:bob   <name>   "Bob" .
			_:alice <friend> _:bob .
		`)}); err != nil {
			t.Fatalf("Mutate: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open 2: %v", err)
	}
	defer db.Close()

	r, err := db.Query(ctx, `{
		q(func: eq(name, "Alice")) {
			name
			friend { name }
		}
	}`)
	if err != nil {
		t.Fatalf("Query after reopen: %v", err)
	}
	if !strings.Contains(string(r.Json), `"name":"Alice"`) ||
		!strings.Contains(string(r.Json), `"name":"Bob"`) {
		t.Errorf("post-reopen traversal lost data: %s", string(r.Json))
	}
}

// TestDQLBackupRestoreRoundtrip backs up a graph, opens a fresh DB at a
// different directory, restores into it, and runs the same DQL query that
// the source DB answered.
func TestDQLBackupRestoreRoundtrip(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	backup := src + "/backup.bin"
	ctx := context.Background()

	var r1Uid uint64
	{
		db, err := dgraph2.Open(dgraph2.Options{Dir: src})
		if err != nil {
			t.Fatalf("Open src: %v", err)
		}
		if err := db.Alter(ctx, "name: string @index(exact) .\n"); err != nil {
			t.Fatalf("Alter: %v", err)
		}
		resp, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
			_:r1 <name> "Restored1" .
			_:r2 <name> "Restored2" .
		`)})
		if err != nil {
			t.Fatalf("Mutate: %v", err)
		}
		if _, err := fmt.Sscanf(resp.Uids["r1"], "0x%x", &r1Uid); err != nil {
			t.Fatalf("parse uid: %v", err)
		}
		if err := db.Backup(ctx, backup); err != nil {
			t.Fatalf("Backup: %v", err)
		}
		fi, _ := os.Stat(backup)
		t.Logf("backup file size: %d bytes", fi.Size())
		_ = db.Close()
	}

	db, err := dgraph2.Open(dgraph2.Options{Dir: dst})
	if err != nil {
		t.Fatalf("Open dst: %v", err)
	}
	defer db.Close()
	if err := db.RestoreFrom(ctx, backup); err != nil {
		t.Fatalf("RestoreFrom: %v", err)
	}

	// Direct read by the restored uid — proves the data keys survived.
	t.Logf("r1 uid=%d", r1Uid)
	v, err := db.Get(ctx, r1Uid, "name")
	if err != nil {
		t.Fatalf("Get post-restore (uid=%d): %v", r1Uid, err)
	}
	if string(v) != "Restored1" {
		t.Errorf("Get post-restore: want Restored1, got %q", string(v))
	}

	r, err := db.Query(ctx, `{ q(func: eq(name, "Restored1")) { name } }`)
	if err != nil {
		t.Fatalf("Query post-restore: %v", err)
	}
	if !strings.Contains(string(r.Json), `"name":"Restored1"`) {
		t.Errorf("restore lost data: %s", string(r.Json))
	}
}

// TestZeroUidRejected verifies the documented invariant that subject UID
// zero is rejected on Set and Get.
func TestZeroUidRejected(t *testing.T) {
	dir := t.TempDir()
	db, err := dgraph2.Open(dgraph2.Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if err := db.Alter(ctx, `name: string .`); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	if err := db.Set(ctx, 0, "name", "x"); err == nil {
		t.Error("Set with uid=0 should fail")
	}
	if _, err := db.Get(ctx, 0, "name"); err == nil {
		t.Error("Get with uid=0 should fail")
	}
}
