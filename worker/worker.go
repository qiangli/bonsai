/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Single-node minimal worker package.
 *
 * Upstream Dgraph's `worker` package was the home of the cluster machinery:
 * Raft, group routing, predicate-move choreography, the inter-alpha gRPC
 * services, the distributed Oracle, and the local query/mutation execution
 * engine all lived together. In dgraph2 the cluster pieces are gone, so
 * `worker` shrinks to a thin façade that:
 *
 *   - holds the Badger handle (`pstore`)
 *   - exposes the API surface the `posting` and `query` packages still call
 *     (`MutateOverNetwork`, `ProcessTaskOverNetwork`, `SortOverNetwork`,
 *     `AssignUidsOverNetwork`, `GetSchemaOverNetwork`, `GetTypes`,
 *     `Init`, `StartRaftNodes`, `MaxLeaseId`, `LimitDefaults`, ...)
 *
 * Most "OverNetwork" entry points currently return ErrNotImplemented — they
 * are wired up but not yet executing real queries/mutations. The remaining
 * phases of the rewrite (P1 task.go port, P3 library API) will fill them in.
 */
package worker

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/dgraph2/protos/pb"
	"github.com/qiangli/dgraph2/schema"
)

// pstore is the Badger handle shared with the posting package.
var pstore *badger.DB

// maxUID tracks the highest UID assigned. Replaces the upstream xidmap +
// Zero lease. Persisted to a Badger key by AssignUidsOverNetwork.
var maxUID uint64

// ErrNotImplemented is the placeholder error returned by query/mutation entry
// points that have not yet been ported to the single-node engine.
var ErrNotImplemented = errors.New("dgraph2: query/mutation engine not yet wired up (P1 work-in-progress)")

// ErrNonExistentTabletMessage matches upstream's error string so that callers
// in `query/query.go` that string-compare against it still work.
const ErrNonExistentTabletMessage = "Requested predicate is not being served by this server."

// MaxLeaseUid is the largest UID we will ever hand out. Matches upstream
// constant so query/mutation.go's overflow check still triggers correctly.
const MaxLeaseUid = uint64(1) << 62

// LimitDefaults mirrors upstream's `--limit` superflag string. Some flags
// (e.g. `mutations`) are read by code paths that survive in dgraph2.
const LimitDefaults = `mutations=allow; query-edge=1000000; normalize-node=10000; ` +
	`mutations-nquad=1000000; disallow-drop=false; query-timeout=0ms; txn-abort-after=5m; ` +
	`max-retries=10; max-pending-queries=10000; shared-instance=false; type-filter-uid-limit=10`

// Options is the runtime configuration for the worker subsystem. ACL,
// encryption, vault, and CDC fields are gone — dgraph2 is local-only.
type Options struct {
	PostingDir   string
	WALDir       string
	MyAddr       string
	HmacSecret   []byte
	LudicrousMode bool
}

// Config is the global, mutable worker configuration. Surviving fields are
// the ones still referenced from the schema/posting layers.
var Config Options

// Init wires the Badger handle into the worker package. Called by
// pkg/dgraph2.Open.
func Init(ps *badger.DB) {
	pstore = ps
}

// Pstore returns the Badger handle. Used by posting/.
func Pstore() *badger.DB { return pstore }

// StartRaftNodes is a no-op in dgraph2; cluster bootstrap is gone.
// Retained as a symbol because tests under query/ still call it.
func StartRaftNodes(_ string) { /* no-op */ }

// BlockingStop tears down worker state. Called by pkg/dgraph2.Close.
func BlockingStop() { /* nothing to drain — no Raft, no goroutines yet */ }

// MaxLeaseId returns the highest UID currently assigned.
func MaxLeaseId() uint64 { return atomic.LoadUint64(&maxUID) }

// SetMaxUID bumps the UID counter, used by Restore and by AssignUids.
func SetMaxUID(uid uint64) {
	for {
		cur := atomic.LoadUint64(&maxUID)
		if uid <= cur || atomic.CompareAndSwapUint64(&maxUID, cur, uid) {
			return
		}
	}
}

// AssignUidsOverNetwork hands out a contiguous block of fresh UIDs.
// Replaces upstream's Zero-leased xidmap with a local atomic counter.
func AssignUidsOverNetwork(_ context.Context, num *pb.Num) (*pb.AssignedIds, error) {
	if num == nil || num.Val == 0 {
		return &pb.AssignedIds{}, nil
	}
	end := atomic.AddUint64(&maxUID, num.Val)
	return &pb.AssignedIds{StartId: end - num.Val + 1, EndId: end}, nil
}

// MutateOverNetwork applies a mutation locally. The full implementation
// (calling embedded.ApplyMutations against the posting store) is deferred
// until P1 of the rewrite is complete.
func MutateOverNetwork(_ context.Context, _ *pb.Mutations) (*api.TxnContext, error) {
	return nil, ErrNotImplemented
}

// ProcessTaskOverNetwork executes a single-predicate task locally.
// Deferred — the upstream implementation in `task.go` is ~2,600 lines and
// still needs to be ported with the cluster forwarding branches stripped.
func ProcessTaskOverNetwork(_ context.Context, _ *pb.Query) (*pb.Result, error) {
	return nil, ErrNotImplemented
}

// SortOverNetwork sorts UIDs by a predicate locally.
func SortOverNetwork(_ context.Context, _ *pb.SortMessage) (*pb.SortResult, error) {
	return nil, ErrNotImplemented
}

// GetSchemaOverNetwork looks up schema definitions from the local schema
// state. Reads the in-memory schema cache populated by Alter.
func GetSchemaOverNetwork(_ context.Context, req *pb.SchemaRequest) ([]*pb.SchemaNode, error) {
	if req == nil {
		return nil, nil
	}
	out := make([]*pb.SchemaNode, 0, len(req.Predicates))
	for _, pred := range req.Predicates {
		su, ok := schema.State().Get(context.Background(), pred)
		if !ok {
			continue
		}
		out = append(out, schemaUpdateToNode(&su))
	}
	return out, nil
}

// GetTypes fetches type definitions by name. Reads the local schema state.
func GetTypes(_ context.Context, req *pb.SchemaRequest) ([]*pb.TypeUpdate, error) {
	if req == nil {
		return nil, nil
	}
	out := make([]*pb.TypeUpdate, 0, len(req.Types))
	for _, name := range req.Types {
		t, ok := schema.State().GetType(name)
		if !ok {
			continue
		}
		out = append(out, &t)
	}
	return out, nil
}

func schemaUpdateToNode(su *pb.SchemaUpdate) *pb.SchemaNode {
	return &pb.SchemaNode{
		Predicate: su.Predicate,
		Type:      su.ValueType.String(),
		Index:     su.Directive == pb.SchemaUpdate_INDEX,
		Tokenizer: su.Tokenizer,
		Reverse:   su.Directive == pb.SchemaUpdate_REVERSE,
		Count:     su.Count,
		List:      su.List,
		Upsert:    su.Upsert,
		Lang:      su.Lang,
	}
}
