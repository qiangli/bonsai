/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// Package dgraph2 is the embeddable Go API for dgraph2, a lightweight,
// local-only fork of upstream Dgraph that drops cluster machinery (Zero, Raft,
// inter-alpha gRPC, group sharding, distributed Oracle, ACL, multi-tenancy,
// at-rest encryption) while keeping the DQL parser, posting-store, schema and
// indexing.
//
// Open returns a *DB backed by an embedded Badger. The DB exposes a small
// surface for schema management and triple-level set/get operations.
//
// The full DQL query path is still being ported (worker.ProcessTaskOverNetwork
// is currently a stub that returns ErrNotImplemented). For now, the library's
// Set/Get/Schema APIs use the posting + schema packages directly. A higher-
// level Query/Mutate that runs DQL over the posting store will land once
// worker/task.go is back from priorart.
package dgraph2

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/proto"

	"github.com/qiangli/dgraph2/posting"
	"github.com/qiangli/dgraph2/schema"
	"github.com/qiangli/dgraph2/types"
	"github.com/qiangli/dgraph2/worker"
	"github.com/qiangli/dgraph2/x"

	"github.com/qiangli/dgraph2/protos/pb"
)

// Options is the configuration for opening a DB.
type Options struct {
	// Dir is the data directory. Badger lives at Dir+"/p".
	Dir string
	// CacheMB is the posting cache size. Defaults to 256MB.
	CacheMB int64
	// EncryptionKey is reserved; dgraph2 currently runs unencrypted.
	EncryptionKey []byte
}

// DB is an open dgraph2 database.
type DB struct {
	mu      sync.Mutex
	closed  atomic.Bool
	opts    Options
	pstore  *badger.DB
	tsCount atomic.Uint64 // local monotonic timestamp generator (replaces Zero Oracle)
}

// Open opens (or creates) a dgraph2 database at the given directory.
//
// On first open, the on-disk Badger is initialised, the worker layer is wired
// to it, and dgraph2's reserved schema is applied at timestamp 1.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, errors.New("dgraph2.Open: Options.Dir is required")
	}
	if opts.CacheMB <= 0 {
		opts.CacheMB = 256
	}

	pdir := filepath.Join(opts.Dir, "p")
	if err := os.MkdirAll(pdir, 0o755); err != nil {
		return nil, fmt.Errorf("dgraph2.Open: cannot create dir %s: %w", pdir, err)
	}

	bopts := badger.DefaultOptions(pdir).
		WithLogger(&x.ToGlog{}).
		WithSyncWrites(false)
	bopts.DetectConflicts = false

	ps, err := badger.OpenManaged(bopts)
	if err != nil {
		return nil, fmt.Errorf("dgraph2.Open: badger open failed: %w", err)
	}

	worker.Init(ps)
	posting.Init(ps, opts.CacheMB<<20, false /* removeOnUpdate */)
	schema.Init(ps)
	if err := schema.LoadFromDb(context.Background()); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("dgraph2.Open: schema load failed: %w", err)
	}

	db := &DB{opts: opts, pstore: ps}
	db.tsCount.Store(2) // 1 is reserved for the initial schema
	if err := db.applyInitialSchema(); err != nil {
		_ = ps.Close()
		return nil, err
	}
	return db, nil
}

// Close flushes and closes the database. It is safe to call multiple times.
func (d *DB) Close() error {
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	posting.Cleanup()
	worker.BlockingStop()
	return d.pstore.Close()
}

// nextTs returns a fresh, locally-monotonic timestamp. Replaces the Zero
// Oracle's distributed timestamp service.
func (d *DB) nextTs() uint64 { return d.tsCount.Add(1) }

// applyInitialSchema seeds the reserved predicates and types that upstream
// Dgraph applies at startup. We use a fixed timestamp of 1 here.
func (d *DB) applyInitialSchema() error {
	const ts = 1
	for _, su := range schema.InitialSchema(x.RootNamespace) {
		if err := d.persistSchema(su, ts); err != nil {
			return err
		}
	}
	for _, t := range schema.InitialTypes(x.RootNamespace) {
		if err := d.persistType(t.GetTypeName(), t, ts); err != nil {
			return err
		}
	}
	return nil
}

// Alter applies a schema string. The string follows DQL schema syntax,
// e.g.: `name: string @index(exact) . age: int .`
func (d *DB) Alter(_ context.Context, schemaText string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	parsed, err := schema.ParseWithNamespace(schemaText, x.RootNamespace)
	if err != nil {
		return fmt.Errorf("Alter: parse failed: %w", err)
	}
	ts := d.nextTs()
	for _, su := range parsed.Preds {
		if err := d.persistSchema(su, ts); err != nil {
			return fmt.Errorf("Alter: persist predicate %q: %w", su.Predicate, err)
		}
	}
	for _, tu := range parsed.Types {
		if err := d.persistType(tu.GetTypeName(), tu, ts); err != nil {
			return fmt.Errorf("Alter: persist type %q: %w", tu.GetTypeName(), err)
		}
	}
	return nil
}

// AssignUid hands out a contiguous block of fresh UIDs.
func (d *DB) AssignUid(_ context.Context, count uint64) (start, end uint64, err error) {
	res, err := worker.AssignUidsOverNetwork(context.Background(), &pb.Num{Val: count})
	if err != nil {
		return 0, 0, err
	}
	return res.StartId, res.EndId, nil
}

// Set writes a single triple <subject> <predicate> <object> at a fresh
// commit timestamp. value is treated according to the schema's value type;
// if no schema entry exists, Set returns an error (Alter the predicate first).
func (d *DB) Set(ctx context.Context, subject uint64, predicate string, value any) error {
	if subject == 0 {
		return errors.New("Set: subject UID must be non-zero")
	}
	su, ok := schema.State().Get(ctx, x.NamespaceAttr(x.RootNamespace, predicate))
	if !ok {
		return fmt.Errorf("Set: no schema for predicate %q (call Alter first)", predicate)
	}

	tid := types.TypeID(su.ValueType)
	val, err := coerce(value, tid)
	if err != nil {
		return err
	}
	bin := types.ValueForType(types.BinaryID)
	if err := types.Marshal(val, &bin); err != nil {
		return fmt.Errorf("Set: marshal value: %w", err)
	}

	startTs := d.nextTs()
	txn := posting.NewTxn(startTs)

	edge := &pb.DirectedEdge{
		Entity:    subject,
		Attr:      x.NamespaceAttr(x.RootNamespace, predicate),
		Value:     bin.Value.([]byte),
		ValueType: pb.Posting_ValType(tid),
		Op:        pb.DirectedEdge_SET,
	}

	key := x.DataKey(edge.Attr, edge.Entity)
	pl, err := txn.Get(key)
	if err != nil {
		return fmt.Errorf("Set: posting list fetch failed: %w", err)
	}
	if err := pl.AddMutationWithIndex(ctx, edge, txn); err != nil {
		return fmt.Errorf("Set: AddMutationWithIndex failed: %w", err)
	}

	// Move the mutations the AddMutationWithIndex pinned in `txn.cache.plists`
	// into `txn.cache.deltas`, which is what CommitToDisk reads from.
	txn.Update()

	commitTs := d.nextTs()
	writer := posting.NewTxnWriter(d.pstore)
	if err := txn.CommitToDisk(writer, commitTs); err != nil {
		return fmt.Errorf("Set: CommitToDisk failed: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("Set: writer flush failed: %w", err)
	}
	posting.Oracle().ProcessDelta(&pb.OracleDelta{
		Txns:        []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
		MaxAssigned: commitTs,
	})
	return nil
}

// Get reads the latest scalar value for <subject> <predicate>.
// Returns (nil, ErrNoValue) when the triple does not exist.
func (d *DB) Get(ctx context.Context, subject uint64, predicate string) ([]byte, error) {
	if subject == 0 {
		return nil, errors.New("Get: subject UID must be non-zero")
	}
	attr := x.NamespaceAttr(x.RootNamespace, predicate)
	if _, ok := schema.State().Get(ctx, attr); !ok {
		return nil, fmt.Errorf("Get: no schema for predicate %q", predicate)
	}

	readTs := d.tsCount.Load()
	key := x.DataKey(attr, subject)
	pl, err := posting.GetNoStore(key, readTs)
	if err != nil {
		return nil, err
	}

	val, err := pl.Value(readTs)
	if err != nil {
		if errors.Is(err, posting.ErrNoValue) {
			return nil, posting.ErrNoValue
		}
		return nil, err
	}
	return val.Value.([]byte), nil
}

// ErrNoValue is returned by Get when the triple does not exist.
var ErrNoValue = posting.ErrNoValue

// Backup writes a full snapshot of the underlying Badger store to dst. The
// resulting file can later be passed to RestoreFrom to seed a new DB.
//
// In upstream Dgraph the backup path coordinated across groups, encrypted
// per-tablet, and produced multi-file manifests; in dgraph2 the database is
// a single embedded Badger, so backup is a single Stream snapshot at the
// current timestamp.
func (d *DB) Backup(ctx context.Context, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("Backup: create %s: %w", dst, err)
	}
	defer func() { _ = f.Close() }()

	stream := d.pstore.NewStreamAt(d.tsCount.Load())
	if _, err := stream.Backup(f, 0); err != nil {
		return fmt.Errorf("Backup: stream: %w", err)
	}
	return f.Sync()
}

// RestoreFrom seeds a freshly opened DB from a backup file produced by
// Backup. The DB must be empty (or at least, not contain data older than the
// backup) — Badger's Load merges the snapshot into the existing key space.
//
// After loading, we advance the local timestamp counter past whatever was
// written in the backup so that fresh writes in this process don't clash
// with restored versions, and so that Get's readTs sees the restored data.
func (d *DB) RestoreFrom(_ context.Context, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("RestoreFrom: open %s: %w", src, err)
	}
	defer func() { _ = f.Close() }()
	if err := d.pstore.Load(f, 16); err != nil {
		return fmt.Errorf("RestoreFrom: %w", err)
	}
	// Advance our local timestamp counter past the backup's high-water mark
	// so reads see restored data and future writes get fresh timestamps.
	if maxV := d.pstore.MaxVersion(); maxV > d.tsCount.Load() {
		d.tsCount.Store(maxV + 1)
	}
	// Tell the posting Oracle the new max-assigned ts so reads at this
	// timestamp do not block in WaitForTs.
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: d.tsCount.Load()})
	// Refresh the in-memory schema cache from disk so subsequent Get/Set
	// calls see the recovered predicate definitions.
	return schema.LoadFromDb(context.Background())
}

// MaxLeaseUid is the upper bound on UIDs that can be assigned. Mirrors
// upstream's invariant.
const MaxLeaseUid = uint64(1) << 62

// persistSchema stores a SchemaUpdate in the in-memory schema state and
// in Badger.
func (d *DB) persistSchema(su *pb.SchemaUpdate, ts uint64) error {
	su = setNamespaceIfMissing(su, x.RootNamespace)

	curr, ok := schema.State().Get(context.Background(), su.Predicate)
	if ok && schemaEqual(&curr, su) {
		return nil
	}
	schema.State().Set(su.Predicate, su)

	w := posting.NewTxnWriter(d.pstore)
	val, err := proto.Marshal(su)
	if err != nil {
		return err
	}
	if err := w.SetAt(x.SchemaKey(su.Predicate), val, posting.BitSchemaPosting, ts); err != nil {
		return err
	}
	return w.Flush()
}

// persistType stores a TypeUpdate in the in-memory schema state and in Badger.
func (d *DB) persistType(name string, t *pb.TypeUpdate, ts uint64) error {
	schema.State().SetType(name, t)
	w := posting.NewTxnWriter(d.pstore)
	val, err := proto.Marshal(t)
	if err != nil {
		return err
	}
	if err := w.SetAt(x.TypeKey(name), val, posting.BitSchemaPosting, ts); err != nil {
		return err
	}
	return w.Flush()
}

// setNamespaceIfMissing prefixes the predicate name with the namespace bytes
// when the caller didn't already include them. x.NamespaceAttr produces names
// of the form `<hex-ns><sep><pred>`, e.g. "0-name". We detect the prefix by
// looking for `<hex-digit>+ NsSeparator` at the start.
func setNamespaceIfMissing(su *pb.SchemaUpdate, ns uint64) *pb.SchemaUpdate {
	if !looksNamespaced(su.Predicate) {
		su.Predicate = x.NamespaceAttr(ns, su.Predicate)
	}
	return su
}

func looksNamespaced(pred string) bool {
	idx := strings.Index(pred, x.NsSeparator)
	if idx <= 0 {
		return false
	}
	for _, r := range pred[:idx] {
		if !isHexDigit(r) {
			return false
		}
	}
	return true
}

func isHexDigit(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

func schemaEqual(a, b *pb.SchemaUpdate) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Predicate == b.Predicate &&
		a.ValueType == b.ValueType &&
		a.Directive == b.Directive &&
		a.Count == b.Count &&
		a.List == b.List &&
		a.Upsert == b.Upsert &&
		a.Lang == b.Lang
}

// coerce converts a Go value into a types.Val of the given TypeID. If the
// caller's value is already in the target type, it is returned directly;
// otherwise we go through types.Convert with a StringID source so the
// scalar→scalar conversion routes work for both int/float/bool/datetime.
func coerce(value any, tid types.TypeID) (types.Val, error) {
	switch v := value.(type) {
	case string:
		if tid == types.StringID || tid == types.DefaultID {
			return types.Val{Tid: tid, Value: v}, nil
		}
		return types.Convert(types.Val{Tid: types.StringID, Value: v}, tid)
	case []byte:
		return types.Val{Tid: types.BinaryID, Value: v}, nil
	case int:
		if tid == types.IntID {
			return types.Val{Tid: tid, Value: int64(v)}, nil
		}
	case int64:
		if tid == types.IntID {
			return types.Val{Tid: tid, Value: v}, nil
		}
	case float64:
		if tid == types.FloatID {
			return types.Val{Tid: tid, Value: v}, nil
		}
	case bool:
		if tid == types.BoolID {
			return types.Val{Tid: tid, Value: v}, nil
		}
	}
	return types.Val{}, fmt.Errorf("coerce: cannot convert %T to %s", value, tid.Name())
}
