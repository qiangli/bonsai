/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package x

import "context"

// In dgraph2 there is only the root namespace. ACL/multi-tenancy is gone, so
// every request runs as namespace 0. These helpers replace the JWT-driven
// namespace extraction in upstream Dgraph.

func ExtractNamespaceFrom(_ context.Context) (uint64, error) {
	return RootNamespace, nil
}

func ParseJWT(_ string) (map[string]interface{}, error) {
	return map[string]interface{}{"namespace": float64(RootNamespace)}, nil
}

// MaybeKeyToBytes is a no-op replacement for the JWT helper. Returns input
// unchanged. Kept so that call sites in edgraph/access.go still compile.
func MaybeKeyToBytes(b []byte) []byte { return b }
