/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import "context"

// dgraph2 supports multi-tenancy without ACL: clients tag requests with a
// namespace ID and the engine isolates the data. ExtractNamespaceFrom
// (and its sibling ExtractNamespace in x.go) read the namespace from the
// gRPC metadata key "namespace" or HTTP request via WithNamespace.
//
// Without an explicit namespace, the request runs in RootNamespace (0).
// This preserves the single-tenant default while letting tenant-aware
// callers route to non-zero namespaces.

// ctxKey is unexported so callers must use WithNamespace / NamespaceFromContext.
type ctxKey int

const namespaceCtxKey ctxKey = 0

// WithNamespace returns a new context with the given namespace set.
// pkg/dgraph2 and the gRPC/HTTP adapters use this to thread the
// per-request namespace down through the worker layer.
func WithNamespace(ctx context.Context, ns uint64) context.Context {
	return context.WithValue(ctx, namespaceCtxKey, ns)
}

// NamespaceFromContext returns the namespace previously stored via
// WithNamespace, or RootNamespace (0) if absent.
func NamespaceFromContext(ctx context.Context) uint64 {
	if v, ok := ctx.Value(namespaceCtxKey).(uint64); ok {
		return v
	}
	return RootNamespace
}

func ExtractNamespaceFrom(ctx context.Context) (uint64, error) {
	return NamespaceFromContext(ctx), nil
}

func ParseJWT(_ string) (map[string]interface{}, error) {
	return map[string]interface{}{"namespace": float64(RootNamespace)}, nil
}

// MaybeKeyToBytes is a no-op replacement for the JWT helper. Returns input
// unchanged. Kept so that call sites in edgraph/access.go still compile.
func MaybeKeyToBytes(b []byte) []byte { return b }
