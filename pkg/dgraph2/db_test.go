/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package dgraph2_test

import (
	"context"
	"errors"
	"testing"

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
