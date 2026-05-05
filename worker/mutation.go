/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Local mutation apply path. Replaces upstream's RaftProposal-based
 * MutateOverNetwork — there is no Raft, no group routing. Each mutation runs
 * against the local Badger directly via posting.Txn / List.AddMutationWithIndex.
 *
 * Ported from priorart/dgraph/worker/mutation.go::runMutation +
 * MutateOverNetwork, with all cluster forwarding removed.
 */
package worker

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/bonsai/posting"
	"github.com/qiangli/bonsai/protos/pb"
	"github.com/qiangli/bonsai/schema"
	"github.com/qiangli/bonsai/types"
	"github.com/qiangli/bonsai/x"
)

// errNonExistentTablet / errUnservedTablet match the upstream errors so call
// sites in task.go that compare them work unchanged. In bonsai every
// predicate lives on the local "tablet" so these are only thrown on truly
// missing predicates.
var (
	errNonExistentTablet = errors.Errorf("%v", ErrNonExistentTabletMessage)
	errUnservedTablet    = errors.Errorf("Tablet isn't being served by this instance")
)

// mutationMu serialises mutation transactions. Upstream relied on Raft order;
// bonsai uses a process-wide lock. Reads still proceed in parallel because
// they don't take this lock.
var mutationMu = newMutationLock()

type mutationLock struct{ ch chan struct{} }

func newMutationLock() *mutationLock {
	return &mutationLock{ch: make(chan struct{}, 1)}
}

func (m *mutationLock) Lock(ctx context.Context) error {
	select {
	case m.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mutationLock) Unlock() { <-m.ch }

func isStarAll(v []byte) bool {
	return string(v) == x.Star
}

func isDeletePredicateEdge(edge *pb.DirectedEdge) bool {
	return edge.Op == pb.DirectedEdge_DEL && isStarAll(edge.Value) &&
		edge.Entity == 0
}

// runMutation applies a single edge to the posting store. Mirrors upstream
// worker/mutation.go::runMutation but without the Raft/cluster paths.
func runMutation(ctx context.Context, edge *pb.DirectedEdge, txn *posting.Txn) error {
	ctx = schema.GetWriteContext(ctx)

	su, ok := schema.State().Get(ctx, edge.Attr)
	if edge.Op != pb.DirectedEdge_DEL {
		if !ok {
			return errors.Errorf("runMutation: no schema for predicate %q", x.ParseAttr(edge.Attr))
		}
	}

	if isDeletePredicateEdge(edge) {
		return errors.New("delete-predicate is not supported in bonsai yet")
	}

	if err := ValidateAndConvert(edge, &su); err != nil {
		return err
	}

	key := x.DataKey(edge.Attr, edge.Entity)

	// The upstream optimisation here picks a getFn based on whether the
	// posting list needs to be read from disk. We use the conservative path
	// (txn.Get) for everything — correct, just slightly slower for
	// scalar-only writes. Can be specialised later.
	pl, err := txn.Get(key)
	if err != nil {
		return err
	}
	return pl.AddMutationWithIndex(ctx, edge, txn)
}

// ValidateAndConvert is a near-verbatim port of the upstream helper. The only
// behavioural changes: vector-keyword check uses the bonsai hnsw constant
// reference (same value); the ACL `dgraph.rule.permission` branch is dropped
// because bonsai has no ACL.
func ValidateAndConvert(edge *pb.DirectedEdge, su *pb.SchemaUpdate) error {
	if isDeletePredicateEdge(edge) {
		return nil
	}
	if types.TypeID(edge.ValueType) == types.DefaultID && isStarAll(edge.Value) {
		return nil
	}
	if strings.Contains(su.Predicate, "__vector_") {
		return errors.Errorf("not allowed to insert mutations in vector index keys, edge: [%v]", edge)
	}

	storageType := posting.TypeID(edge)
	schemaType := types.TypeID(su.ValueType)

	switch {
	case edge.Lang != "" && !su.GetLang():
		return errors.Errorf("attr [%v] needs @lang directive to accept lang-tagged edge: [%v]",
			x.ParseAttr(edge.Attr), edge)
	case !schemaType.IsScalar() && !storageType.IsScalar():
		return nil
	case !schemaType.IsScalar() && storageType.IsScalar():
		return errors.Errorf("input for uid predicate %q is scalar; edge: %v",
			x.ParseAttr(edge.Attr), edge)
	case schemaType.IsScalar() && !storageType.IsScalar():
		return errors.Errorf("input for scalar predicate %q is uid; edge: %v",
			x.ParseAttr(edge.Attr), edge)
	case storageType == schemaType && schemaType != types.DefaultID:
		return nil
	case schemaType == types.DefaultID:
		schemaType = storageType
	}

	src := types.Val{Tid: types.TypeID(edge.ValueType), Value: edge.Value}
	dst, err := types.Convert(src, schemaType)
	if err != nil {
		return err
	}
	b := types.ValueForType(types.BinaryID)
	if err := types.Marshal(dst, &b); err != nil {
		return err
	}
	edge.ValueType = schemaType.Enum()
	var ok bool
	edge.Value, ok = b.Value.([]byte)
	if !ok {
		return errors.Errorf("conversion %v -> %v failed", storageType, schemaType)
	}
	return nil
}

// MutateOverNetwork applies a mutation to the local store. Replaces the
// upstream cluster-routed implementation with a serialised local apply.
//
// The flow is:
//  1. Allocate startTs.
//  2. Apply schema/type updates.
//  3. Walk edges; runMutation each one through a fresh posting.Txn.
//  4. Allocate commitTs and CommitToDisk via a TxnWriter.
//  5. Tell the posting Oracle the txn committed so subsequent reads are
//     visible at >= commitTs.
func MutateOverNetwork(ctx context.Context, m *pb.Mutations) (*api.TxnContext, error) {
	if m == nil {
		return &api.TxnContext{}, nil
	}
	if pstore == nil {
		return nil, errors.New("worker: pstore not initialised; call Init first")
	}

	if err := mutationMu.Lock(ctx); err != nil {
		return nil, err
	}
	defer mutationMu.Unlock()

	startTs := nextLocalTs()

	// Apply schema updates first so subsequent edges see the new types.
	for _, su := range m.Schema {
		if err := updateSchemaLocal(su, startTs); err != nil {
			return nil, errors.Wrapf(err, "schema update %q", su.Predicate)
		}
	}
	for _, tu := range m.Types {
		if err := updateTypeLocal(tu, startTs); err != nil {
			return nil, errors.Wrapf(err, "type update %q", tu.TypeName)
		}
	}

	if len(m.Edges) > 0 {
		txn := posting.NewTxn(startTs)
		for _, edge := range m.Edges {
			if err := runMutation(ctx, edge, txn); err != nil {
				return nil, err
			}
		}
		txn.Update()

		commitTs := nextLocalTs()
		writer := posting.NewTxnWriter(pstore)
		if err := txn.CommitToDisk(writer, commitTs); err != nil {
			return nil, errors.Wrap(err, "CommitToDisk")
		}
		if err := writer.Flush(); err != nil {
			return nil, errors.Wrap(err, "writer.Flush")
		}
		posting.Oracle().ProcessDelta(&pb.OracleDelta{
			Txns:        []*pb.TxnStatus{{StartTs: startTs, CommitTs: commitTs}},
			MaxAssigned: commitTs,
		})
		return &api.TxnContext{StartTs: startTs, CommitTs: commitTs}, nil
	}

	return &api.TxnContext{StartTs: startTs, CommitTs: startTs}, nil
}

// updateSchemaLocal persists a schema update to both the in-memory schema
// state and Badger. bonsai doesn't have the upstream Raft proposal, so the
// caller is expected to be running under mutationMu.
func updateSchemaLocal(su *pb.SchemaUpdate, ts uint64) error {
	schema.State().Set(su.Predicate, su)
	w := posting.NewTxnWriter(pstore)
	val, err := proto.Marshal(su)
	if err != nil {
		return err
	}
	if err := w.SetAt(x.SchemaKey(su.Predicate), val, posting.BitSchemaPosting, ts); err != nil {
		return err
	}
	return w.Flush()
}

// ApplyInitialSchema persists the reserved (`dgraph.*`) schema and type
// definitions for the given namespace. Called by pkg/bonsai.Open for
// the root namespace and by CreateNamespace for new tenants.
func ApplyInitialSchema(ns, ts uint64) error {
	for _, su := range schema.InitialSchema(ns) {
		if err := updateSchemaLocal(su, ts); err != nil {
			return err
		}
	}
	for _, t := range schema.InitialTypes(ns) {
		if _, ok := schema.State().GetType(t.GetTypeName()); ok {
			continue
		}
		if err := updateTypeLocal(t, ts); err != nil {
			return err
		}
	}
	return nil
}

func updateTypeLocal(tu *pb.TypeUpdate, ts uint64) error {
	schema.State().SetType(tu.TypeName, tu)
	w := posting.NewTxnWriter(pstore)
	val, err := proto.Marshal(tu)
	if err != nil {
		return err
	}
	if err := w.SetAt(x.TypeKey(tu.TypeName), val, posting.BitSchemaPosting, ts); err != nil {
		return err
	}
	return w.Flush()
}

// localTs is the single source of truth for transaction timestamps in
// bonsai. We never call posting.Oracle().ProcessDelta concurrently — the
// Oracle uses a CompareAndSwap on its maxAssigned counter and asserts on
// failure, so racing ProcessDelta calls panic. We advance the Oracle
// serially, only after CommitToDisk completes.
var localTs uint64

// nextLocalTs returns a fresh timestamp. Both startTs and commitTs allocate
// from this counter, ensuring commitTs > startTs naturally.
//
// At Open time, pkg/bonsai seeds localTs from pstore.MaxVersion+1; from
// then on, mutationMu serialises increments so concurrent mutations don't
// reuse a timestamp.
func nextLocalTs() uint64 {
	return atomic.AddUint64(&localTs, 1)
}

// NextTs is the public version of nextLocalTs, exposed so pkg/bonsai can
// share the same atomic counter for its non-Mutate write paths
// (DB.Set, DB.Alter, DB.AssignUid persistence). Keeping a single counter
// avoids the dual-counter bug where reads at d.tsCount blocked forever in
// Oracle.WaitForTs because worker had advanced past it.
func NextTs() uint64 {
	return atomic.AddUint64(&localTs, 1)
}

// CurrentTs returns the current high-water without advancing it.
func CurrentTs() uint64 {
	return atomic.LoadUint64(&localTs)
}

// SeedLocalTs is called by pkg/bonsai.Open to seed the local timestamp
// counter from the recovered Badger MaxVersion. Must be called before any
// mutation is processed.
func SeedLocalTs(ts uint64) {
	atomic.StoreUint64(&localTs, ts)
}
