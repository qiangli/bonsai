/*
 * SPDX-FileCopyrightText: dgraph2 contributors
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

	"github.com/qiangli/dgraph2/posting"
	"github.com/qiangli/dgraph2/protos/pb"
	"github.com/qiangli/dgraph2/schema"
	"github.com/qiangli/dgraph2/types"
	"github.com/qiangli/dgraph2/x"
)

// errNonExistentTablet / errUnservedTablet match the upstream errors so call
// sites in task.go that compare them work unchanged. In dgraph2 every
// predicate lives on the local "tablet" so these are only thrown on truly
// missing predicates.
var (
	errNonExistentTablet = errors.Errorf("%v", ErrNonExistentTabletMessage)
	errUnservedTablet    = errors.Errorf("Tablet isn't being served by this instance")
)

// mutationMu serialises mutation transactions. Upstream relied on Raft order;
// dgraph2 uses a process-wide lock. Reads still proceed in parallel because
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
		return errors.New("delete-predicate is not supported in dgraph2 yet")
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
// behavioural changes: vector-keyword check uses the dgraph2 hnsw constant
// reference (same value); the ACL `dgraph.rule.permission` branch is dropped
// because dgraph2 has no ACL.
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
// state and Badger. dgraph2 doesn't have the upstream Raft proposal, so the
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

// nextLocalTs hands out timestamps for mutations. Stays in worker so the
// posting layer's Oracle can be advanced consistently. dgraph2's pkg/dgraph2
// has its own counter for the user-facing API; both must stay aligned with
// pstore.MaxVersion. We use a process-global atomic counter seeded by the
// posting Oracle's MaxAssigned.
func nextLocalTs() uint64 {
	for {
		cur := posting.Oracle().MaxAssigned()
		next := cur + 1
		// We don't have a CAS on MaxAssigned, so just announce the new
		// timestamp to the Oracle by recording a phantom delta. Subsequent
		// commits will overwrite this with their real commitTs.
		posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: next})
		if posting.Oracle().MaxAssigned() >= next {
			return next
		}
	}
}

// Compile-time guarantee that we're using atomic from somewhere; satisfies
// the linter when nextLocalTs is the only export.
var _ = atomic.LoadUint64
