/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// Package bonsai is the embeddable Go API for bonsai, a lightweight,
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
package bonsai

import (
	"context"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	badgerpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/dgo/v250/protos/api"
	apipb "github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/ristretto/v2/z"
	"google.golang.org/protobuf/proto"

	"github.com/qiangli/bonsai/chunker"
	"github.com/qiangli/bonsai/dql"
	"github.com/qiangli/bonsai/pkg/audit"
	"github.com/qiangli/bonsai/posting"
	"github.com/qiangli/bonsai/query"
	"github.com/qiangli/bonsai/schema"
	"github.com/qiangli/bonsai/types"
	"github.com/qiangli/bonsai/worker"
	"github.com/qiangli/bonsai/x"

	"github.com/qiangli/bonsai/protos/pb"
)

// Options is the configuration for opening a DB.
type Options struct {
	// Dir is the data directory. Badger lives at Dir+"/p".
	Dir string
	// CacheMB is the posting cache size. Defaults to 256MB.
	CacheMB int64
	// EncryptionKey is reserved; bonsai currently runs unencrypted.
	EncryptionKey []byte
	// AuditLog, if non-nil, receives one entry per administrative or write
	// operation (Mutate, Alter, Drop*, namespace, backup, restore). A nil
	// logger is a cheap no-op at every call site.
	AuditLog *audit.Logger
	// ReadOnly opens the DB without write capability. Mutate, Alter, Drop*,
	// CreateNamespace and the auto-schema mutator path all return an error.
	// Used by build-once / query-many deployments and by `OpenFrozen`.
	ReadOnly bool
	// ValueLogGCInterval controls the background Badger value-log GC cadence.
	// Default 10 minutes. Set to a negative value to disable; the embedded
	// caller can then run db.RunValueLogGC manually.
	ValueLogGCInterval time.Duration
	// CompactOnClose forces a final L0→bottom compaction during Close so
	// the on-disk artifact is tight. Useful right after a bulk load and
	// before producing a frozen artifact.
	CompactOnClose bool
	// AutoSchema infers a permissive schema entry on the fly when Mutate
	// targets an unknown predicate. Edges with a uid object become
	// `[uid]`; literal-valued edges become `string`. Off by default so
	// strict-schema users keep their write-time validation; turn on for
	// schemaless / fast-evolving codebases (e.g. graph-export ingest).
	AutoSchema bool
}

// ErrReadOnly is returned from any write path when Options.ReadOnly is true.
var ErrReadOnly = errors.New("bonsai: database opened read-only")

// DB is an open bonsai database.
type DB struct {
	mu      sync.Mutex
	closed  atomic.Bool
	opts    Options
	pstore  *badger.DB
	tsCount atomic.Uint64 // local monotonic timestamp generator (replaces Zero Oracle)

	// mutationTick advances on every successful or attempted write op
	// (Mutate, Upsert, Drop*, RestoreFrom, BackupTo with side effects).
	// Subscription watchers poll this to detect "something changed" without
	// the DB needing fan-out plumbing.
	mutationTick atomic.Uint64

	// gcStop signals the background value-log GC ticker to exit at Close.
	// nil if Options.ValueLogGCInterval was negative or ReadOnly is set.
	gcStop chan struct{}

	// frozenTemp is set by OpenFrozen to the temp directory the artifact
	// was extracted into; removed during Close. Empty for ordinary opens.
	frozenTemp string
}

// MutationTick returns a counter that increments on every write or admin
// op. Subscribers compare ticks across polls to decide whether the
// underlying state may have changed.
func (d *DB) MutationTick() uint64 { return d.mutationTick.Load() }

// ReadOnly reports whether this DB was opened in read-only mode.
func (d *DB) ReadOnly() bool { return d.opts.ReadOnly }

// guardWrite returns ErrReadOnly when the DB was opened with
// Options.ReadOnly. Callers prepend `if err := d.guardWrite(); err != nil`
// to every write/admin entry point.
func (d *DB) guardWrite() error {
	if d.opts.ReadOnly {
		return ErrReadOnly
	}
	return nil
}

// NextReadableTs returns the current high-water timestamp — the maximum
// of the local DB counter and the worker oracle. Callers can capture
// this as a "save point" and later pass it to QueryAsOf to see the
// state that was visible at that moment.
func (d *DB) NextReadableTs() uint64 {
	ts := d.tsCount.Load()
	if oraTs := posting.Oracle().MaxAssigned(); oraTs > ts {
		ts = oraTs
	}
	return ts
}

// Open opens (or creates) a bonsai database at the given directory.
//
// On first open, the on-disk Badger is initialised, the worker layer is wired
// to it, and bonsai's reserved schema is applied at timestamp 1.
func Open(opts Options) (*DB, error) {
	if opts.Dir == "" {
		return nil, errors.New("bonsai.Open: Options.Dir is required")
	}
	if opts.CacheMB <= 0 {
		opts.CacheMB = 256
	}

	pdir := filepath.Join(opts.Dir, "p")
	if err := os.MkdirAll(pdir, 0o755); err != nil {
		return nil, fmt.Errorf("bonsai.Open: cannot create dir %s: %w", pdir, err)
	}

	bopts := badger.DefaultOptions(pdir).
		WithLogger(&x.ToGlog{}).
		WithSyncWrites(false)
	// Conflict detection is on. bonsai serialises all mutations behind a
	// process-wide lock in worker/mutation.go and assigns monotonic
	// timestamps from worker.localTs, so in practice no conflicts can fire,
	// but Badger now tracks the read/write sets and will surface a
	// y.ErrConflict if the invariant is ever broken.
	bopts.DetectConflicts = true
	if opts.ReadOnly {
		bopts = bopts.WithReadOnly(true).WithBypassLockGuard(true)
	}

	ps, err := badger.OpenManaged(bopts)
	if err != nil {
		return nil, fmt.Errorf("bonsai.Open: badger open failed: %w", err)
	}

	// IndexRebuild uses os.MkdirTemp(x.WorkerConfig.TmpDir, ...) with TmpDir
	// blank by default. Set it to the OS temp dir so rebuild paths work.
	if x.WorkerConfig.TmpDir == "" {
		x.WorkerConfig.TmpDir = os.TempDir()
	}

	// x.Config is a process-global with zero defaults. The query engine
	// treats "0" as a hard limit (e.g. shortest/recurse abort with "Exceeded
	// query edge limit = 0"), so seed sensible values matching upstream.
	if x.Config.LimitQueryEdge == 0 {
		x.Config.LimitQueryEdge = 1e6
	}
	if x.Config.LimitMutationsNquad == 0 {
		x.Config.LimitMutationsNquad = 1e6
	}
	if x.Config.LimitNormalizeNode == 0 {
		x.Config.LimitNormalizeNode = 1e4
	}
	if x.Config.QueryTimeout == 0 {
		x.Config.QueryTimeout = 60 * time.Second
	}
	if x.Config.MaxRetries == 0 {
		x.Config.MaxRetries = -1
	}

	worker.Init(ps)
	posting.Init(ps, opts.CacheMB<<20, false /* removeOnUpdate */)
	schema.Init(ps)
	if err := schema.LoadFromDb(context.Background()); err != nil {
		_ = ps.Close()
		return nil, fmt.Errorf("bonsai.Open: schema load failed: %w", err)
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
		return nil, fmt.Errorf("bonsai.Open: read uid counter: %w", err)
	}

	// Tell the posting Oracle about the timestamp high-water so reads at
	// the current ts don't block in WaitForTs.
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: db.tsCount.Load()})

	if !opts.ReadOnly {
		if err := db.applyInitialSchema(); err != nil {
			_ = ps.Close()
			return nil, err
		}
	}

	// Background value-log GC. Default 10 min; negative disables; ReadOnly
	// short-circuits because Badger errors on RunValueLogGC in read-only.
	gcInterval := opts.ValueLogGCInterval
	if gcInterval == 0 {
		gcInterval = 10 * time.Minute
	}
	if gcInterval > 0 && !opts.ReadOnly {
		db.gcStop = make(chan struct{})
		go db.valueLogGCLoop(gcInterval)
	}
	return db, nil
}

// valueLogGCLoop periodically asks Badger to reclaim space from the
// value log. RunValueLogGC returns badger.ErrNoRewrite when nothing to
// reclaim — that's the steady state, not an error.
func (d *DB) valueLogGCLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-d.gcStop:
			return
		case <-t.C:
			// Ratio 0.5: rewrite a vlog file when 50% of its data is
			// stale. Loop until we hit ErrNoRewrite or anything else.
			for {
				if err := d.pstore.RunValueLogGC(0.5); err != nil {
					break
				}
			}
		}
	}
}

// uidCounterKey is the reserved Badger key holding the highest assigned UID.
// bonsai-specific; never clashes with x.DataKey/IndexKey/etc. because those
// always start with byte prefixes 0x00..0x0c.
var uidCounterKey = []byte("__bonsai_max_uid")

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

// auditDeferred returns a closure suitable for `defer ...()` that records
// the operation's final error and elapsed time into the audit log AND
// advances mutationTick if the operation is a write. Pass a pointer to the
// function's named-return error so the deferred function observes its
// post-update value.
func (d *DB) auditDeferred(op string, ctx context.Context, args map[string]any, errPtr *error) func() {
	start := time.Now()
	return func() {
		// Bump the mutation tick on any successful write/admin op so
		// subscribers (pkg/graphql) can poll for state changes.
		var e error
		if errPtr != nil {
			e = *errPtr
		}
		if e == nil && isWriteOp(op) {
			d.mutationTick.Add(1)
		}
		if d.opts.AuditLog != nil {
			d.opts.AuditLog.LogErr(op, x.NamespaceFromContext(ctx), args, start, e)
		}
	}
}

// isWriteOp returns true for operations that change the DB state. Reads
// (Query, Get) are intentionally not in this list.
func isWriteOp(op string) bool {
	switch op {
	case "Mutate", "Upsert", "Alter",
		"DropAll", "DropData", "DropPredicate", "DropType",
		"CreateNamespace", "DropNamespace",
		"RestoreFrom", "RestoreFromManifest":
		return true
	}
	return false
}

// Close flushes and closes the database. It is safe to call multiple times.
//
// When Options.CompactOnClose is true, a final L0→bottom Badger flatten
// runs before the underlying store closes. Callers running build-once /
// query-many workloads enable this once after their final mutation so
// the resulting on-disk artifact is tight.
func (d *DB) Close() error {
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	if d.gcStop != nil {
		close(d.gcStop)
		d.gcStop = nil
	}
	posting.Cleanup()
	worker.BlockingStop()
	if d.opts.CompactOnClose && !d.opts.ReadOnly {
		// Best-effort: a Flatten failure during shutdown is logged but
		// not surfaced — the user wanted compaction, not a hard error.
		if err := d.pstore.Flatten(2); err != nil {
			// Use the package logger pattern via x.ToGlog{}; here we
			// just swallow because Close is shutdown-time.
			_ = err
		}
	}
	closeErr := d.pstore.Close()
	if d.frozenTemp != "" {
		// Best-effort cleanup of the extracted frozen artifact.
		_ = os.RemoveAll(d.frozenTemp)
		d.frozenTemp = ""
	}
	return closeErr
}

// nextTs returns a fresh, locally-monotonic timestamp. Backed by
// worker.NextTs so the worker.MutateOverNetwork path and pkg/bonsai's
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
func (d *DB) Alter(ctx context.Context, schemaText string) (err error) {
	if err := d.guardWrite(); err != nil {
		return err
	}
	defer d.auditDeferred("Alter", ctx, map[string]any{
		"schema_bytes": len(schemaText),
	}, &err)()
	d.mu.Lock()
	defer d.mu.Unlock()

	ns := x.NamespaceFromContext(ctx)
	parsed, err := schema.ParseWithNamespace(schemaText, ns)
	if err != nil {
		return fmt.Errorf("Alter: parse failed: %w", err)
	}
	ts := d.nextTs()
	for _, su := range parsed.Preds {
		su = setNamespaceIfMissing(su, ns)

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
	// Publish the new high-water mark to the posting Oracle. Without this,
	// a Query issued after Alter picks readTs >= ts but Oracle.MaxAssigned
	// is still at the previous Mutate's commit ts, so processTask blocks
	// in WaitForTs forever. Same pattern as the Mutate-failure path
	// patched earlier in worker/mutation.go.
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: ts})
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
	return d.QueryWithVarsAsOf(ctx, 0, q, vars)
}

// QueryAsOf is Query against the database as it looked at readTs. Pass 0
// for readTs to read at the current timestamp (same as Query). Bonsai
// keeps all versions in Badger, so any past timestamp the engine has
// observed is readable; ts beyond the current high-water mark is rejected.
//
// Use cases: forensics ("what did this node look like 5 minutes ago?"),
// reproducible reports against a specific snapshot, debugging state at
// the moment a mutation landed.
func (d *DB) QueryAsOf(ctx context.Context, readTs uint64, q string) (*api.Response, error) {
	return d.QueryWithVarsAsOf(ctx, readTs, q, nil)
}

// QueryWithVarsAsOf is QueryWithVars with an explicit readTs (0 = current).
func (d *DB) QueryWithVarsAsOf(ctx context.Context, readTs uint64, q string, vars map[string]string) (*api.Response, error) {
	parsed, err := dql.Parse(dql.Request{Str: q, Variables: vars})
	if err != nil {
		return nil, fmt.Errorf("Query: parse: %w", err)
	}

	curTs := d.tsCount.Load()
	if oraTs := posting.Oracle().MaxAssigned(); oraTs > curTs {
		curTs = oraTs
	}
	if readTs == 0 {
		readTs = curTs
	} else if readTs > curTs {
		return nil, fmt.Errorf("QueryAsOf: readTs %d exceeds current %d", readTs, curTs)
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

// Upsert runs an upsert: query + mutations where the mutation can refer to
// query variables via uid(varname). The DQL parser handles the bundled
// syntax already; here we run the query, populate variables from the
// SubGraph results, then walk the mutation's nquads substituting `uid(v)`
// references with the discovered uids.
//
// Supported subset: query + a single mutation block; uid(v) substitution
// in subject and object positions of N-Quad mutations. More exotic upsert
// patterns (multiple mutations, conditional mutations) require additional
// glue.
func (d *DB) Upsert(ctx context.Context, queryDQL string, m *apipb.Mutation) (resp *api.Response, err error) {
	if err := d.guardWrite(); err != nil {
		return nil, err
	}
	defer d.auditDeferred("Upsert", ctx, map[string]any{
		"query_bytes":  len(queryDQL),
		"cond":         m.GetCond(),
		"set_bytes":    len(m.GetSetNquads()) + len(m.GetSetJson()),
		"delete_bytes": len(m.GetDelNquads()) + len(m.GetDeleteJson()),
	}, &err)()
	// Parse the query, telling the parser that any vars it defines will be
	// used by the bundled mutation. Without needVars the parser rejects
	// var-only queries with "Some variables are defined but not used".
	needVars := scanVarNamesFromMutation(m)
	parsed, err := dql.ParseWithNeedVars(dql.Request{Str: queryDQL}, needVars)
	if err != nil {
		return nil, fmt.Errorf("Upsert: parse: %w", err)
	}
	readTs := worker.CurrentTs()
	if oraTs := posting.Oracle().MaxAssigned(); oraTs > readTs {
		readTs = oraTs
	}
	req := &query.Request{
		ReadTs:   readTs,
		Latency:  &query.Latency{Start: timeNow()},
		DqlQuery: &parsed,
	}
	if err := req.ProcessQuery(ctx); err != nil {
		return nil, fmt.Errorf("Upsert: process: %w", err)
	}
	// Build a JSON-form query response too, so callers can inspect what
	// matched.
	queryJson, err := query.ToJson(ctx, req.Latency, req.Subgraphs)
	if err != nil {
		return nil, fmt.Errorf("Upsert: ToJson: %w", err)
	}
	queryResp := &api.Response{Json: queryJson}

	// Collect uid bindings from req.Vars (populated by ProcessQuery). Each
	// var has a Uids list; we substitute the first uid as the canonical
	// case for "find existing or create" upsert.
	uidVars := map[string]uint64{}
	for name, v := range req.Vars {
		if v.Uids != nil && len(v.Uids.Uids) > 0 {
			uidVars[name] = v.Uids.Uids[0]
		}
	}

	// Substitute uid(v) tokens in mutation NQuads. The serialized RDF form
	// lets uid(varname) appear where a UID would; we string-replace before
	// handing to the parser. This is a pragmatic approximation of the
	// upstream upsert path.
	if len(uidVars) > 0 {
		m = substituteUidVars(m, uidVars)
	}
	mr, err := d.Mutate(ctx, m)
	if err != nil {
		return nil, err
	}
	// Stitch the query response and mutate response together.
	mr.Json = queryResp.Json
	return mr, nil
}

// scanVarNamesFromMutation pulls out `uid(name)` tokens from the mutation
// nquad bytes; those names are what the upsert query is expected to
// define. We hand them to ParseWithNeedVars so the parser doesn't fail
// the "defined but not used" check.
func scanVarNamesFromMutation(m *apipb.Mutation) []string {
	if m == nil {
		return nil
	}
	seen := map[string]struct{}{}
	scan := func(b []byte) {
		s := string(b)
		for {
			i := strings.Index(s, "uid(")
			if i < 0 {
				break
			}
			s = s[i+4:]
			j := strings.Index(s, ")")
			if j < 0 {
				break
			}
			name := strings.TrimSpace(s[:j])
			if name != "" {
				seen[name] = struct{}{}
			}
			s = s[j+1:]
		}
	}
	scan(m.SetNquads)
	scan(m.DelNquads)
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	return out
}


// substituteUidVars replaces `uid(varname)` tokens in the mutation's
// SetNquads/DelNquads with the resolved 0xN uid for that variable.
func substituteUidVars(m *apipb.Mutation, vars map[string]uint64) *apipb.Mutation {
	out := proto.Clone(m).(*apipb.Mutation)
	out.SetNquads = substVars(m.SetNquads, vars)
	out.DelNquads = substVars(m.DelNquads, vars)
	return out
}

func substVars(rdf []byte, vars map[string]uint64) []byte {
	if len(rdf) == 0 || len(vars) == 0 {
		return rdf
	}
	s := string(rdf)
	for name, uid := range vars {
		s = strings.ReplaceAll(s, "uid("+name+")", fmt.Sprintf("<0x%x>", uid))
	}
	return []byte(s)
}

// Mutate applies a batch of triples to the database. The mutation can be
// supplied as RDF N-Quads (`m.SetNquads` / `m.DelNquads`) or, in future
// versions, as JSON. Blank-node identifiers (`_:alice`) are resolved to fresh
// UIDs via the worker UID counter and a per-call substitution map.
//
// On success the returned api.Response.Uids reports the assigned UIDs for
// each blank node.
func (d *DB) Mutate(ctx context.Context, m *api.Mutation) (resp *api.Response, err error) {
	if err := d.guardWrite(); err != nil {
		return nil, err
	}
	defer d.auditDeferred("Mutate", ctx, map[string]any{
		"set_nquads_bytes":  len(m.GetSetNquads()),
		"del_nquads_bytes":  len(m.GetDelNquads()),
		"set_json_bytes":    len(m.GetSetJson()),
		"delete_json_bytes": len(m.GetDeleteJson()),
		"set_count":         len(m.GetSet()),
		"del_count":         len(m.GetDel()),
		"cond":              m.GetCond(),
	}, &err)()
	if m == nil {
		return &api.Response{}, nil
	}
	resp = &api.Response{Uids: map[string]string{}}

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
	// Already-parsed NQuads via m.Set / m.Del (used by bonsai-bulk and
	// bonsai-live, which chunk RDF/JSON in the loader and hand the parsed
	// triples straight to Mutate).
	for _, q := range m.Set {
		nquads = append(nquads, taggedNQ{q: q, delete: false})
	}
	for _, q := range m.Del {
		nquads = append(nquads, taggedNQ{q: q, delete: true})
	}
	// JSON mutations.
	if len(m.SetJson) > 0 {
		nq, _, err := chunker.ParseJSON(m.SetJson, chunker.SetNquads)
		if err != nil {
			return nil, fmt.Errorf("Mutate: parse SetJson: %w", err)
		}
		for _, q := range nq {
			nquads = append(nquads, taggedNQ{q: q, delete: false})
		}
	}
	if len(m.DeleteJson) > 0 {
		nq, _, err := chunker.ParseJSON(m.DeleteJson, chunker.DeleteNquads)
		if err != nil {
			return nil, fmt.Errorf("Mutate: parse DeleteJson: %w", err)
		}
		for _, q := range nq {
			nquads = append(nquads, taggedNQ{q: q, delete: true})
		}
	}

	// AutoSchema: when Options.AutoSchema is set, infer a permissive
	// schema for any predicate that hasn't been declared yet. Edges with
	// a non-empty ObjectId become `[uid]`; everything else becomes
	// `string`. The inferred schema is the safe default — users can
	// promote later via an explicit Alter (e.g. add @index, change list
	// to scalar). This mirrors the upgrade path the auto-detect import
	// already takes; AutoSchema lifts that mechanism to general Mutate.
	if d.opts.AutoSchema && len(nquads) > 0 {
		raw := make([]*apipb.NQuad, 0, len(nquads))
		for _, t := range nquads {
			raw = append(raw, t.q)
		}
		if err := d.autoSchemaFor(ctx, raw); err != nil {
			return nil, fmt.Errorf("Mutate: auto-schema: %w", err)
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
	ns := x.NamespaceFromContext(ctx)
	mutations := &pb.Mutations{}
	for _, t := range nquads {
		dq := dql.NQuad{NQuad: t.q}
		edge, err := dq.ToEdgeUsing(xidMap)
		if err != nil {
			return nil, fmt.Errorf("Mutate: edge: %w", err)
		}
		edge.Attr = x.NamespaceAttr(ns, edge.Attr)
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

// autoSchemaFor scans parsed nquads, identifies predicates not yet in the
// schema state, and applies a minimal Alter to declare them. Called from
// Mutate when Options.AutoSchema is true. Predicates already declared are
// left untouched (so a user-curated schema with @index/@reverse stays
// authoritative).
func (d *DB) autoSchemaFor(ctx context.Context, nquads []*apipb.NQuad) error {
	ns := x.NamespaceFromContext(ctx)
	state := schema.State()
	if state == nil {
		return nil
	}
	type inferred struct {
		predicate string
		isUid     bool
	}
	seen := map[string]inferred{}
	for _, q := range nquads {
		if q == nil || q.Predicate == "" {
			continue
		}
		bare := q.Predicate
		nsAttr := x.NamespaceAttr(ns, bare)
		if _, ok := state.Get(ctx, nsAttr); ok {
			continue
		}
		isUid := q.ObjectId != ""
		if existing, dup := seen[bare]; dup {
			if existing.isUid != isUid {
				// Predicate appeared as both uid and value within the
				// same Mutate. Pick the value form; uid is more
				// restrictive and the user can promote later.
				existing.isUid = false
				seen[bare] = existing
			}
			continue
		}
		seen[bare] = inferred{predicate: bare, isUid: isUid}
	}
	if len(seen) == 0 {
		return nil
	}
	var sb strings.Builder
	for _, inf := range seen {
		if inf.isUid {
			fmt.Fprintf(&sb, "%s: [uid] .\n", inf.predicate)
		} else {
			fmt.Fprintf(&sb, "%s: string .\n", inf.predicate)
		}
	}
	// Re-using the public Alter so AutoSchema entries also appear in the
	// audit log — operators see exactly what predicates the writes
	// implicitly declared. Mutate already passed guardWrite; the
	// duplicate check inside Alter is a few-ns no-op.
	return d.Alter(ctx, sb.String())
}

// Export writes the database in RDF or JSON form to a directory. The
// resulting files can be re-loaded via the bulk loader (when restored)
// or simple RDF replay through Mutate(SetNquads).
//
// Format: "rdf" emits N-Quads, one triple per line. "json" emits a JSON
// array of {subject, predicate, value} records.
//
// bonsai export is a single-file dump (no group iteration / chunking
// like upstream); fine for the embedded-DB use case.
func (d *DB) Export(ctx context.Context, format, dst string) error {
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("Export: create: %w", err)
	}
	defer func() { _ = f.Close() }()
	if err := d.ExportTo(ctx, format, f); err != nil {
		return err
	}
	return f.Sync()
}

// ExportTo writes the database in the requested format to an arbitrary
// writer (file, HTTP response body, *bytes.Buffer in tests). Format
// values:
//
//	rdf   N-Quads, one triple per line, in Badger's LSM iteration order
//	json  JSON array of {subject,predicate,value|object_id}
//	ntx   "Bonsai N-Quads" — canonical N-Quads with deterministic ordering
//	      (sorted by subject UID, then predicate, then object). Designed
//	      for `git diff`-friendly snapshots; two ntx exports of the same
//	      logical state are byte-identical.
func (d *DB) ExportTo(ctx context.Context, format string, w io.Writer) error {
	switch format {
	case "rdf":
		return d.exportRDF(ctx, w)
	case "json":
		return d.exportJSON(ctx, w)
	case "ntx":
		return d.exportNTX(ctx, w)
	default:
		return fmt.Errorf("Export: unknown format %q (want rdf, json, or ntx)", format)
	}
}

// exportNTX emits a canonical, deterministic N-Quads dump. We buffer the
// triples in memory and sort them by (subject, predicate, object) before
// writing. This trades streaming for diff-friendliness — fine for the
// embedded-DB scale (millions of triples max); for huge graphs use the
// streaming `rdf` format instead.
func (d *DB) exportNTX(ctx context.Context, w io.Writer) error {
	readTs := worker.CurrentTs()
	if oraTs := posting.Oracle().MaxAssigned(); oraTs > readTs {
		readTs = oraTs
	}

	type triple struct{ subj, line string }
	var (
		mu      sync.Mutex
		triples []triple
	)
	stream := d.pstore.NewStreamAt(readTs)
	stream.Prefix = []byte{x.ByteData}
	stream.KeyToList = func(key []byte, _ *badger.Iterator) (*badgerpb.KVList, error) {
		pk, err := x.Parse(key)
		if err != nil {
			return nil, err
		}
		if !pk.IsData() {
			return nil, nil
		}
		pl, err := posting.GetNoStore(key, readTs)
		if err != nil {
			return nil, err
		}
		_, attr := x.ParseNamespaceAttr(pk.Attr)
		_ = pl.Iterate(readTs, 0, func(p *pb.Posting) error {
			var buf bytes.Buffer
			emitRDFPosting(&buf, pk.Uid, attr, p)
			line := buf.String()
			mu.Lock()
			triples = append(triples, triple{
				subj: fmt.Sprintf("0x%020x|%s|%s", pk.Uid, attr, line),
				line: line,
			})
			mu.Unlock()
			return nil
		})
		return nil, nil
	}
	stream.Send = func(_ *z.Buffer) error { return nil }
	if err := stream.Orchestrate(ctx); err != nil {
		return fmt.Errorf("Export: orchestrate: %w", err)
	}
	sort.Slice(triples, func(i, j int) bool {
		return triples[i].subj < triples[j].subj
	})
	for _, t := range triples {
		if _, err := io.WriteString(w, t.line); err != nil {
			return err
		}
	}
	return nil
}

func (d *DB) exportRDF(ctx context.Context, w io.Writer) error {
	readTs := worker.CurrentTs()
	if oraTs := posting.Oracle().MaxAssigned(); oraTs > readTs {
		readTs = oraTs
	}

	// Walk all data keys via Badger stream. For each posting list we
	// iterate every posting (not just the single-value head) so uid-list
	// predicates emit one edge per neighbour rather than getting skipped.
	stream := d.pstore.NewStreamAt(readTs)
	stream.Prefix = []byte{x.ByteData}
	stream.KeyToList = func(key []byte, _ *badger.Iterator) (*badgerpb.KVList, error) {
		pk, err := x.Parse(key)
		if err != nil {
			return nil, err
		}
		if !pk.IsData() {
			return nil, nil
		}
		pl, err := posting.GetNoStore(key, readTs)
		if err != nil {
			return nil, err
		}
		_, attr := x.ParseNamespaceAttr(pk.Attr)
		_ = pl.Iterate(readTs, 0, func(p *pb.Posting) error {
			emitRDFPosting(w, pk.Uid, attr, p)
			return nil
		})
		return nil, nil
	}
	stream.Send = func(_ *z.Buffer) error { return nil }
	if err := stream.Orchestrate(ctx); err != nil {
		return fmt.Errorf("Export: orchestrate: %w", err)
	}
	return nil
}

// emitRDFPosting writes one N-Quads line for a single posting. UID edges
// go out as `<subj> <pred> <obj-uid> .` and scalar values pick up a typed
// literal via postingRDFLiteral.
func emitRDFPosting(w io.Writer, subj uint64, attr string, p *pb.Posting) {
	switch p.PostingType {
	case pb.Posting_REF:
		fmt.Fprintf(w, "<0x%x> <%s> <0x%x> .\n", subj, attr, p.Uid)
	default:
		fmt.Fprintf(w, "<0x%x> <%s> %s", subj, attr, postingRDFLiteral(p))
		if p.LangTag != nil {
			fmt.Fprintf(w, "@%s", string(p.LangTag))
		}
		fmt.Fprintln(w, " .")
	}
}

// postingRDFLiteral formats a non-REF posting's binary value into an RDF
// literal. Posting.Value is already the binary form Badger stored, so we
// unmarshal it through types.Marshal into a string for textual types and
// fall back to the typed-literal form for numeric / temporal / geo / vector.
func postingRDFLiteral(p *pb.Posting) string {
	tid := types.TypeID(p.ValType)
	src := types.Val{Tid: types.BinaryID, Value: p.Value}
	conv, err := types.Convert(src, tid)
	if err != nil {
		// Fall back to a quoted hex dump rather than dropping the value.
		return fmt.Sprintf("%q", string(p.Value))
	}
	switch tid {
	case types.StringID, types.DefaultID:
		return fmt.Sprintf("%q", anyToString(conv.Value))
	case types.IntID:
		return fmt.Sprintf(`"%d"^^<xs:int>`, conv.Value.(int64))
	case types.FloatID:
		return fmt.Sprintf(`"%g"^^<xs:float>`, conv.Value.(float64))
	case types.BoolID:
		return fmt.Sprintf(`"%v"^^<xs:boolean>`, conv.Value.(bool))
	case types.DateTimeID:
		// Use string form via the type system's Marshal path.
		dst := types.ValueForType(types.StringID)
		if err := types.Marshal(conv, &dst); err == nil {
			return fmt.Sprintf(`%q^^<xs:dateTime>`, anyToString(dst.Value))
		}
		return fmt.Sprintf("%q", anyToString(conv.Value))
	case types.GeoID:
		dst := types.ValueForType(types.StringID)
		if err := types.Marshal(conv, &dst); err == nil {
			return fmt.Sprintf(`%q^^<geo:geojson>`, anyToString(dst.Value))
		}
		return fmt.Sprintf("%q", anyToString(conv.Value))
	case types.VFloatID:
		dst := types.ValueForType(types.StringID)
		if err := types.Marshal(conv, &dst); err == nil {
			return fmt.Sprintf(`%q^^<float32vector>`, anyToString(dst.Value))
		}
		return fmt.Sprintf("%q", anyToString(conv.Value))
	default:
		return fmt.Sprintf("%q", anyToString(conv.Value))
	}
}

// anyToString accepts whichever shape types.Convert returns (string or
// []byte) and yields a Go string.
func anyToString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func (d *DB) exportJSON(ctx context.Context, w io.Writer) error {
	_, _ = w.Write([]byte("["))
	first := true

	readTs := worker.CurrentTs()
	if oraTs := posting.Oracle().MaxAssigned(); oraTs > readTs {
		readTs = oraTs
	}

	stream := d.pstore.NewStreamAt(readTs)
	stream.Prefix = []byte{x.ByteData}
	stream.KeyToList = func(key []byte, _ *badger.Iterator) (*badgerpb.KVList, error) {
		pk, err := x.Parse(key)
		if err != nil {
			return nil, err
		}
		if !pk.IsData() {
			return nil, nil
		}
		pl, err := posting.GetNoStore(key, readTs)
		if err != nil {
			return nil, err
		}
		_, attr := x.ParseNamespaceAttr(pk.Attr)
		_ = pl.Iterate(readTs, 0, func(p *pb.Posting) error {
			if !first {
				_, _ = w.Write([]byte(","))
			}
			first = false
			switch p.PostingType {
			case pb.Posting_REF:
				fmt.Fprintf(w, `{"subject":"0x%x","predicate":%q,"object_id":"0x%x"}`,
					pk.Uid, attr, p.Uid)
			default:
				val := types.Val{Tid: types.TypeID(p.ValType), Value: p.Value}
				conv, err := types.Convert(val, val.Tid)
				if err != nil {
					conv = val
				}
				fmt.Fprintf(w, `{"subject":"0x%x","predicate":%q,"value":%s}`,
					pk.Uid, attr, jsonValue(conv))
			}
			return nil
		})
		return nil, nil
	}
	stream.Send = func(_ *z.Buffer) error { return nil }
	if err := stream.Orchestrate(ctx); err != nil {
		return fmt.Errorf("Export: orchestrate: %w", err)
	}
	_, _ = w.Write([]byte("]"))
	return nil
}

func formatRDFValue(v types.Val) string {
	switch v.Tid {
	case types.StringID, types.DefaultID:
		return fmt.Sprintf("%q", string(v.Value.([]byte)))
	case types.IntID:
		return fmt.Sprintf(`"%d"^^<xs:int>`, v.Value.(int64))
	case types.FloatID:
		return fmt.Sprintf(`"%g"^^<xs:float>`, v.Value.(float64))
	case types.BoolID:
		return fmt.Sprintf(`"%v"^^<xs:boolean>`, v.Value.(bool))
	default:
		return fmt.Sprintf(`"%v"`, v.Value)
	}
}

func jsonValue(v types.Val) string {
	bin := types.ValueForType(types.StringID)
	if err := types.Marshal(v, &bin); err != nil {
		return `null`
	}
	if s, ok := bin.Value.(string); ok {
		// JSON-quote.
		b, _ := json.Marshal(s)
		return string(b)
	}
	return `null`
}

// CreateNamespace seeds a new tenant: applies the reserved initial schema
// for `ns` and tracks it in the namespace registry. Returns an error if
// the namespace already exists.
//
// Without ACL, anyone with server access can call this; the semantics are
// "tenant routing", not "tenant isolation with auth".
func (d *DB) CreateNamespace(ctx context.Context, ns uint64) (err error) {
	if err := d.guardWrite(); err != nil { return err }
	defer d.auditDeferred("CreateNamespace", ctx, map[string]any{"ns": ns}, &err)()
	d.mu.Lock()
	defer d.mu.Unlock()
	existing, err := d.listNamespacesLocked()
	if err != nil {
		return fmt.Errorf("CreateNamespace: read registry: %w", err)
	}
	for _, e := range existing {
		if e == ns {
			return fmt.Errorf("CreateNamespace: namespace %d already exists", ns)
		}
	}
	ts := worker.NextTs()
	if err := worker.ApplyInitialSchema(ns, ts); err != nil {
		return fmt.Errorf("CreateNamespace: apply initial schema: %w", err)
	}
	return d.writeNamespaceRegistry(append(existing, ns))
}

// DropNamespace tears down a tenant: drops every Badger key prefixed with
// the namespace, plus the schema and type entries.
func (d *DB) DropNamespace(ctx context.Context, ns uint64) (err error) {
	if err := d.guardWrite(); err != nil { return err }
	defer d.auditDeferred("DropNamespace", ctx, map[string]any{"ns": ns}, &err)()
	if ns == x.RootNamespace {
		return fmt.Errorf("DropNamespace: cannot drop root namespace")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	prefix := x.NamespaceToBytes(ns)
	// Every binary key in bonsai starts with: [prefix-byte][8 ns bytes][...].
	// We have to drop each prefix-class separately.
	for _, kind := range []byte{x.ByteData, x.ByteIndex, x.ByteReverse, x.ByteCount, x.ByteCountRev, x.ByteSchema, x.ByteType} {
		keyPrefix := append([]byte{kind}, prefix...)
		if err := d.pstore.DropPrefix(keyPrefix); err != nil {
			return fmt.Errorf("DropNamespace: drop prefix %x: %w", keyPrefix, err)
		}
	}
	posting.ResetCache()

	// Strip schema-state entries in this namespace.
	for _, attr := range schema.State().Predicates() {
		nsOfAttr, _ := x.ParseNamespaceAttr(attr)
		if nsOfAttr == ns {
			_ = schema.State().Delete(attr, worker.NextTs())
		}
	}
	for _, t := range schema.State().Types() {
		// Type names are stored without namespace prefix in upstream;
		// we conservatively don't auto-delete types here. Caller can
		// DropType explicitly if needed.
		_ = t
	}

	existing, err := d.listNamespacesLocked()
	if err != nil {
		return fmt.Errorf("DropNamespace: read registry: %w", err)
	}
	out := existing[:0]
	for _, e := range existing {
		if e != ns {
			out = append(out, e)
		}
	}
	return d.writeNamespaceRegistry(out)
}

// ListNamespaces returns every namespace ID known to the database (always
// includes RootNamespace).
func (d *DB) ListNamespaces(ctx context.Context) ([]uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.listNamespacesLocked()
}

var namespaceRegistryKey = []byte("__bonsai_namespaces")

func (d *DB) listNamespacesLocked() ([]uint64, error) {
	txn := d.pstore.NewTransactionAt(d.pstore.MaxVersion(), false)
	defer txn.Discard()
	it, err := txn.Get(namespaceRegistryKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return []uint64{x.RootNamespace}, nil
	}
	if err != nil {
		return nil, err
	}
	var out []uint64
	err = it.Value(func(v []byte) error {
		if len(v)%8 != 0 {
			return fmt.Errorf("namespace registry: bad length %d", len(v))
		}
		for i := 0; i < len(v); i += 8 {
			out = append(out, binary.BigEndian.Uint64(v[i:i+8]))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	hasRoot := false
	for _, n := range out {
		if n == x.RootNamespace {
			hasRoot = true
			break
		}
	}
	if !hasRoot {
		out = append([]uint64{x.RootNamespace}, out...)
	}
	return out, nil
}

func (d *DB) writeNamespaceRegistry(nss []uint64) error {
	buf := make([]byte, 0, 8*len(nss))
	for _, n := range nss {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], n)
		buf = append(buf, b[:]...)
	}
	wb := d.pstore.NewManagedWriteBatch()
	defer wb.Cancel()
	if err := wb.SetEntryAt(&badger.Entry{Key: namespaceRegistryKey, Value: buf}, worker.NextTs()); err != nil {
		return err
	}
	return wb.Flush()
}

// DropAll wipes every key from Badger and re-applies the reserved schema.
// Equivalent to upstream's `Operation{DropAll: true}`.
func (d *DB) DropAll(ctx context.Context) (err error) {
	if err := d.guardWrite(); err != nil { return err }
	defer d.auditDeferred("DropAll", ctx, nil, &err)()
	if err := d.pstore.DropAll(); err != nil {
		return fmt.Errorf("DropAll: %w", err)
	}
	posting.ResetCache()
	schema.State().DeleteAll()
	worker.SeedLocalTs(1)
	d.tsCount.Store(1)
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: 1})
	return d.applyInitialSchema()
}

// DropData wipes data while preserving the schema. Drops all DataKey,
// IndexKey, ReverseKey, CountKey prefixes; keeps SchemaKey + TypeKey.
func (d *DB) DropData(ctx context.Context) (err error) {
	if err := d.guardWrite(); err != nil { return err }
	defer d.auditDeferred("DropData", ctx, nil, &err)()
	prefixes := [][]byte{
		{x.ByteData},
		{x.ByteIndex},
		{x.ByteReverse},
		{x.ByteCount},
		{x.ByteCountRev},
	}
	if err := d.pstore.DropPrefix(prefixes...); err != nil {
		return fmt.Errorf("DropData: %w", err)
	}
	posting.ResetCache()
	return nil
}

// DropPredicate removes all data and indexes for one predicate. The
// argument can be the bare name ("name") or already-namespaced
// ("0-name"); we coerce to the namespaced form here.
func (d *DB) DropPredicate(ctx context.Context, predicate string) (err error) {
	if err := d.guardWrite(); err != nil { return err }
	defer d.auditDeferred("DropPredicate", ctx, map[string]any{"predicate": predicate}, &err)()
	attr := predicate
	if !looksNamespaced(attr) {
		attr = x.NamespaceAttr(x.RootNamespace, attr)
	}
	pk := x.ParsedKey{Attr: attr}
	prefixes := [][]byte{
		pk.DataPrefix(),
		pk.IndexPrefix(),
		pk.ReversePrefix(),
		pk.CountPrefix(true),
		pk.CountPrefix(false),
	}
	if err := d.pstore.DropPrefix(prefixes...); err != nil {
		return fmt.Errorf("DropPredicate: %w", err)
	}
	if err := schema.State().Delete(attr, d.nextTs()); err != nil {
		return fmt.Errorf("DropPredicate: schema delete: %w", err)
	}
	posting.ResetCache()
	return nil
}

// DropType removes a type definition by name (does not affect predicate
// data). The type definition is the schema-language `type T { ... }`
// declaration.
func (d *DB) DropType(ctx context.Context, typeName string) (err error) {
	if err := d.guardWrite(); err != nil { return err }
	defer d.auditDeferred("DropType", ctx, map[string]any{"type": typeName}, &err)()
	if err := schema.State().DeleteType(typeName, d.nextTs()); err != nil {
		return fmt.Errorf("DropType: %w", err)
	}
	return nil
}

// SchemaText returns the active schema in DQL form, suitable for handing
// back to Alter on a fresh DB. Reserved (dgraph.* / 0-dgraph.*) predicates
// are filtered out.
func (d *DB) SchemaText(ctx context.Context) (string, error) {
	var b strings.Builder
	for _, attr := range schema.State().Predicates() {
		bare := attr
		if i := strings.Index(attr, x.NsSeparator); i > 0 {
			bare = attr[i+1:]
		}
		if strings.HasPrefix(bare, "dgraph.") {
			continue
		}
		su, ok := schema.State().Get(ctx, attr)
		if !ok {
			continue
		}
		b.WriteString(formatSchemaUpdate(bare, &su))
		b.WriteString("\n")
	}
	for _, t := range schema.State().Types() {
		if strings.Contains(t, "dgraph.") {
			continue
		}
		b.WriteString("type ")
		b.WriteString(t)
		b.WriteString(" {\n")
		tu, ok := schema.State().GetType(t)
		if ok {
			for _, f := range tu.Fields {
				bare := f.Predicate
				if i := strings.Index(bare, x.NsSeparator); i > 0 {
					bare = bare[i+1:]
				}
				b.WriteString("  ")
				b.WriteString(bare)
				b.WriteString("\n")
			}
		}
		b.WriteString("}\n")
	}
	return b.String(), nil
}

func formatSchemaUpdate(name string, su *pb.SchemaUpdate) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteString(": ")
	tn := types.TypeID(su.ValueType).Name()
	if su.List {
		b.WriteString("[")
		b.WriteString(tn)
		b.WriteString("]")
	} else {
		b.WriteString(tn)
	}
	if len(su.Tokenizer) > 0 {
		b.WriteString(" @index(")
		b.WriteString(strings.Join(su.Tokenizer, ", "))
		b.WriteString(")")
	}
	if su.Directive == pb.SchemaUpdate_REVERSE {
		b.WriteString(" @reverse")
	}
	if su.Count {
		b.WriteString(" @count")
	}
	if su.Upsert {
		b.WriteString(" @upsert")
	}
	if su.Lang {
		b.WriteString(" @lang")
	}
	b.WriteString(" .")
	return b.String()
}

// MaxUid returns the current high-water UID. Used by /admin/state.
func (d *DB) MaxUid() uint64 { return worker.MaxLeaseId() }

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
	ns := x.NamespaceFromContext(ctx)
	attr := x.NamespaceAttr(ns, predicate)
	su, ok := schema.State().Get(ctx, attr)
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
		Attr:      attr,
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
	ns := x.NamespaceFromContext(ctx)
	attr := x.NamespaceAttr(ns, predicate)
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
// per-tablet, and produced multi-file manifests; in bonsai the database is
// a single embedded Badger, so backup is a single Stream snapshot at the
// current timestamp.
//
// We read the snapshot at the maximum of the local DB counter and the
// worker timestamp counter (which is what mutations advance). Reading at a
// stale ts misses freshly-committed mutations that hadn't propagated to the
// DB-side counter yet.
func (d *DB) Backup(ctx context.Context, dst string) (err error) {
	defer d.auditDeferred("Backup", ctx, map[string]any{"dst": dst}, &err)()
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
func (d *DB) RestoreFrom(ctx context.Context, src string) (err error) {
	if err := d.guardWrite(); err != nil { return err }
	defer d.auditDeferred("RestoreFrom", ctx, map[string]any{"src": src}, &err)()
	_ = ctx // ctx is consumed by auditDeferred; the body below doesn't need it
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
