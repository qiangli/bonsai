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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v250/protos/api"
	apipb "github.com/dgraph-io/dgo/v250/protos/api"
	"google.golang.org/protobuf/proto"

	"github.com/qiangli/dgraph2/chunker"
	"github.com/qiangli/dgraph2/dql"
	"github.com/qiangli/dgraph2/posting"
	"github.com/qiangli/dgraph2/query"
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

	// IndexRebuild uses os.MkdirTemp(x.WorkerConfig.TmpDir, ...) with TmpDir
	// blank by default. Set it to the OS temp dir so rebuild paths work.
	if x.WorkerConfig.TmpDir == "" {
		x.WorkerConfig.TmpDir = os.TempDir()
	}

	worker.Init(ps)
	posting.Init(ps, opts.CacheMB<<20, false /* removeOnUpdate */)
	schema.Init(ps)
	if err := schema.LoadFromDb(context.Background()); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("dgraph2.Open: schema load failed: %w", err)
	}

	db := &DB{opts: opts, pstore: ps}

	// Resume the local timestamp counter past whatever's already on disk so
	// fresh writes get monotonically increasing timestamps and reads see the
	// most recent committed data. On first open MaxVersion is 0.
	seed := ps.MaxVersion()
	if seed < 1 {
		seed = 1 // 1 is reserved for the initial schema
	}
	db.tsCount.Store(seed)
	worker.SeedLocalTs(seed)

	// Resume the UID counter from the persisted high-water mark.
	if uid, err := readUidCounter(ps); err == nil {
		worker.SetMaxUID(uid)
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		_ = ps.Close()
		return nil, fmt.Errorf("dgraph2.Open: read uid counter: %w", err)
	}

	// Tell the posting Oracle about the timestamp high-water so reads at
	// the current ts don't block in WaitForTs.
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: db.tsCount.Load()})

	if err := db.applyInitialSchema(); err != nil {
		_ = ps.Close()
		return nil, err
	}
	return db, nil
}

// uidCounterKey is the reserved Badger key holding the highest assigned UID.
// dgraph2-specific; never clashes with x.DataKey/IndexKey/etc. because those
// always start with byte prefixes 0x00..0x0c.
var uidCounterKey = []byte("__dgraph2_max_uid")

func readUidCounter(ps *badger.DB) (uint64, error) {
	txn := ps.NewTransactionAt(ps.MaxVersion(), false)
	defer txn.Discard()
	it, err := txn.Get(uidCounterKey)
	if err != nil {
		return 0, err
	}
	var uid uint64
	err = it.Value(func(v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("uid counter: bad length %d", len(v))
		}
		uid = binary.BigEndian.Uint64(v)
		return nil
	})
	return uid, err
}

func (d *DB) writeUidCounter(uid uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uid)
	wb := d.pstore.NewManagedWriteBatch()
	defer wb.Cancel()
	if err := wb.SetEntryAt(&badger.Entry{Key: uidCounterKey, Value: buf[:]}, d.nextTs()); err != nil {
		return err
	}
	return wb.Flush()
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

// nextTs returns a fresh, locally-monotonic timestamp. Backed by
// worker.NextTs so the worker.MutateOverNetwork path and pkg/dgraph2's
// direct Set path share a single, monotonically increasing counter. Reads
// in Get/Query that take readTs from this counter will always see prior
// commits without blocking in Oracle.WaitForTs.
func (d *DB) nextTs() uint64 {
	ts := worker.NextTs()
	d.tsCount.Store(ts)
	return ts
}

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
//
// When a predicate's index/reverse/count directives change, Alter rebuilds
// the indexes by dropping the old prefixes from Badger and re-tokenising
// every existing data key.
func (d *DB) Alter(ctx context.Context, schemaText string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	parsed, err := schema.ParseWithNamespace(schemaText, x.RootNamespace)
	if err != nil {
		return fmt.Errorf("Alter: parse failed: %w", err)
	}
	ts := d.nextTs()
	for _, su := range parsed.Preds {
		su = setNamespaceIfMissing(su, x.RootNamespace)

		// Snapshot the old schema BEFORE we overwrite it; needed to decide
		// whether indexes have to be rebuilt. We `proto.Clone` rather than
		// take an address of the value the schema state hands us, because
		// pb.SchemaUpdate embeds a sync.Mutex via the proto runtime and
		// `&value` would be a copylocks vet warning.
		var oldPtr *pb.SchemaUpdate
		if old, ok := schema.State().Get(ctx, su.Predicate); ok {
			oldPtr = proto.Clone(&old).(*pb.SchemaUpdate)
		}

		if err := d.persistSchemaUpdate(su, ts); err != nil {
			return fmt.Errorf("Alter: persist predicate %q: %w", su.Predicate, err)
		}

		rb := posting.IndexRebuild{
			Attr:          su.Predicate,
			StartTs:       ts,
			OldSchema:     oldPtr,
			CurrentSchema: su,
		}
		if rb.NeedIndexRebuild() {
			if err := rb.DropIndexes(ctx); err != nil {
				return fmt.Errorf("Alter: drop indexes for %q: %w", su.Predicate, err)
			}
			if err := rb.BuildIndexes(ctx); err != nil {
				return fmt.Errorf("Alter: build indexes for %q: %w", su.Predicate, err)
			}
		}
	}
	for _, tu := range parsed.Types {
		if err := d.persistType(tu.GetTypeName(), tu, ts); err != nil {
			return fmt.Errorf("Alter: persist type %q: %w", tu.GetTypeName(), err)
		}
	}
	return nil
}

// persistSchemaUpdate is persistSchema without the namespace coercion;
// callers must pre-namespace su.Predicate.
func (d *DB) persistSchemaUpdate(su *pb.SchemaUpdate, ts uint64) error {
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

// Query runs a DQL query against the database. It parses the query through
// the dql package, builds a SubGraph, runs ProcessQuery (which calls
// worker.ProcessTaskOverNetwork — the local executor ported from upstream's
// task.go), and returns the JSON-encoded result on api.Response.Json.
//
// The result format matches upstream Dgraph's DQL output (the GraphQL
// formatter was dropped during the rewrite).
func (d *DB) Query(ctx context.Context, q string) (*api.Response, error) {
	return d.QueryWithVars(ctx, q, nil)
}

// QueryWithVars is Query with bound variables, e.g. `query Q($name: string) {
// q(func: eq(name, $name)) {uid}}` with vars `{"$name": "Alice"}`.
func (d *DB) QueryWithVars(ctx context.Context, q string, vars map[string]string) (*api.Response, error) {
	parsed, err := dql.Parse(dql.Request{Str: q, Variables: vars})
	if err != nil {
		return nil, fmt.Errorf("Query: parse: %w", err)
	}

	readTs := d.tsCount.Load()
	if oraTs := posting.Oracle().MaxAssigned(); oraTs > readTs {
		readTs = oraTs
	}
	latency := &query.Latency{Start: timeNow()}
	req := &query.Request{
		ReadTs:   readTs,
		Latency:  latency,
		DqlQuery: &parsed,
	}
	if err := req.ProcessQuery(ctx); err != nil {
		return nil, fmt.Errorf("Query: process: %w", err)
	}
	out, err := query.ToJson(ctx, latency, req.Subgraphs)
	if err != nil {
		return nil, fmt.Errorf("Query: ToJson: %w", err)
	}
	return &api.Response{Json: out}, nil
}

// timeNow is a hook so tests can stub time. Default uses time.Now().
var timeNow = func() time.Time { return time.Now() }

// Mutate applies a batch of triples to the database. The mutation can be
// supplied as RDF N-Quads (`m.SetNquads` / `m.DelNquads`) or, in future
// versions, as JSON. Blank-node identifiers (`_:alice`) are resolved to fresh
// UIDs via the worker UID counter and a per-call substitution map.
//
// On success the returned api.Response.Uids reports the assigned UIDs for
// each blank node.
func (d *DB) Mutate(ctx context.Context, m *api.Mutation) (*api.Response, error) {
	if m == nil {
		return &api.Response{}, nil
	}
	resp := &api.Response{Uids: map[string]string{}}

	type taggedNQ struct {
		q      *apipb.NQuad
		delete bool
	}
	var nquads []taggedNQ
	if len(m.SetNquads) > 0 {
		nq, _, err := chunker.ParseRDFs(m.SetNquads)
		if err != nil {
			return nil, fmt.Errorf("Mutate: parse SetNquads: %w", err)
		}
		for _, q := range nq {
			nquads = append(nquads, taggedNQ{q: q, delete: false})
		}
	}
	if len(m.DelNquads) > 0 {
		nq, _, err := chunker.ParseRDFs(m.DelNquads)
		if err != nil {
			return nil, fmt.Errorf("Mutate: parse DelNquads: %w", err)
		}
		for _, q := range nq {
			nquads = append(nquads, taggedNQ{q: q, delete: true})
		}
	}

	// Substitute blank-node references with fresh UIDs.
	xidMap := map[string]uint64{}
	var blanks []string
	for _, t := range nquads {
		for _, key := range []string{t.q.Subject, t.q.ObjectId} {
			if isBlankNode(key) {
				if _, ok := xidMap[key]; !ok {
					xidMap[key] = 0 // placeholder
					blanks = append(blanks, key)
				}
			}
		}
	}
	if len(blanks) > 0 {
		start, _, err := d.AssignUid(ctx, uint64(len(blanks)))
		if err != nil {
			return nil, fmt.Errorf("Mutate: AssignUid: %w", err)
		}
		for i, b := range blanks {
			xidMap[b] = start + uint64(i)
			resp.Uids[strings.TrimPrefix(b, "_:")] = fmt.Sprintf("0x%x", start+uint64(i))
		}
	}

	// Build edges; route through worker.MutateOverNetwork.
	mutations := &pb.Mutations{}
	for _, t := range nquads {
		dq := dql.NQuad{NQuad: t.q}
		edge, err := dq.ToEdgeUsing(xidMap)
		if err != nil {
			return nil, fmt.Errorf("Mutate: edge: %w", err)
		}
		edge.Attr = x.NamespaceAttr(x.RootNamespace, edge.Attr)
		if t.delete {
			edge.Op = pb.DirectedEdge_DEL
		}
		mutations.Edges = append(mutations.Edges, edge)
	}

	tctx, err := worker.MutateOverNetwork(ctx, mutations)
	if err != nil {
		return nil, err
	}
	resp.Txn = &api.TxnContext{StartTs: tctx.StartTs, CommitTs: tctx.CommitTs}
	return resp, nil
}

func isBlankNode(s string) bool { return strings.HasPrefix(s, "_:") }

// AssignUid hands out a contiguous block of fresh UIDs and persists the new
// high-water mark to Badger so the counter survives restart.
func (d *DB) AssignUid(_ context.Context, count uint64) (start, end uint64, err error) {
	res, err := worker.AssignUidsOverNetwork(context.Background(), &pb.Num{Val: count})
	if err != nil {
		return 0, 0, err
	}
	if err := d.writeUidCounter(res.EndId); err != nil {
		return 0, 0, fmt.Errorf("AssignUid: persist counter: %w", err)
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
//
// The read timestamp is the higher of the local DB counter and the posting
// Oracle's MaxAssigned, because mutations may be routed through the
// pkg-global worker.MutateOverNetwork path which advances the Oracle but not
// this DB's counter.
func (d *DB) Get(ctx context.Context, subject uint64, predicate string) ([]byte, error) {
	if subject == 0 {
		return nil, errors.New("Get: subject UID must be non-zero")
	}
	attr := x.NamespaceAttr(x.RootNamespace, predicate)
	if _, ok := schema.State().Get(ctx, attr); !ok {
		return nil, fmt.Errorf("Get: no schema for predicate %q", predicate)
	}

	readTs := d.tsCount.Load()
	if oraTs := posting.Oracle().MaxAssigned(); oraTs > readTs {
		readTs = oraTs
	}
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
//
// We read the snapshot at the maximum of the local DB counter and the
// worker timestamp counter (which is what mutations advance). Reading at a
// stale ts misses freshly-committed mutations that hadn't propagated to the
// DB-side counter yet.
func (d *DB) Backup(ctx context.Context, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("Backup: create %s: %w", dst, err)
	}
	defer func() { _ = f.Close() }()

	readTs := d.tsCount.Load()
	if w := worker.CurrentTs(); w > readTs {
		readTs = w
	}
	stream := d.pstore.NewStreamAt(readTs)
	if _, err := stream.Backup(f, 0); err != nil {
		return fmt.Errorf("Backup: stream: %w", err)
	}
	return f.Sync()
}

// RestoreFrom seeds a freshly opened DB from a backup file produced by
// Backup. Badger's Load calls Prepare(), which wipes existing keys, so the
// destination DB must be the open we want the snapshot to replace.
//
// After loading, we advance the local timestamp counter past the backup's
// high-water mark, refresh the posting cache (the old in-memory entries
// point at keys that Prepare dropped), and reload the schema state.
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
	maxV := d.pstore.MaxVersion()
	// Conservative: even if MaxVersion under-reports after Load, advance to
	// at least worker.CurrentTs+1 so future writes don't collide.
	target := maxV + 1
	if cur := worker.CurrentTs() + 1; cur > target {
		target = cur
	}
	d.tsCount.Store(target)
	worker.SeedLocalTs(target)

	// Drop the posting layer's in-memory cache, which still references the
	// keys Badger.Load.Prepare() just dropped.
	posting.ResetCache()

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
