/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Minimal GraphQL endpoint for dgraph2.
 *
 * Scope (intentional):
 *   - Translate top-level GraphQL queries to DQL and return GraphQL-shape JSON.
 *   - Translate top-level GraphQL mutations (`add<Type>`) to RDF + db.Mutate.
 *   - Use the DQL types already defined via Alter — no separate SDL upload.
 *   - Surface gqlparser errors as `{"errors":[...]}` per GraphQL HTTP spec.
 *
 * Out of scope (would land in a future Wave):
 *   - SDL upload + persistence at /admin/schema/graphql
 *   - @auth / @custom / @lambda directives (cluster/enterprise features)
 *   - Subscriptions (require websockets + live query plumbing)
 *   - Full filter/order/pagination DSL — only `id`/`func: type()` + nested
 *     field expansion are wired today
 *   - Schema introspection (returns minimal placeholder)
 *
 * Mapping rules:
 *
 *   query {
 *     queryPerson { name age friend { name } }
 *   }
 *      ↓
 *   { q(func: type(Person)) { name age friend { name } } }
 *
 *   query {
 *     getPerson(id: "0x1") { name }
 *   }
 *      ↓
 *   { q(func: uid(0x1)) { name } }
 *
 *   mutation {
 *     addPerson(input: {name: "Alice", age: 30}) { uid }
 *   }
 *      ↓
 *   _:new <name> "Alice" .
 *   _:new <age>  "30"^^<xs:int> .
 *   _:new <dgraph.type> "Person" .
 */

package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/dgraph-io/gqlparser/v2/ast"
	"github.com/dgraph-io/gqlparser/v2/parser"

	"github.com/qiangli/dgraph2/pkg/dgraph2"
)

// Request is the standard GraphQL POST body.
type Request struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName,omitempty"`
	Variables     map[string]any `json:"variables,omitempty"`
}

// Response is the standard GraphQL response shape.
type Response struct {
	Data   map[string]any `json:"data,omitempty"`
	Errors []errorEntry   `json:"errors,omitempty"`
}

type errorEntry struct {
	Message string `json:"message"`
	Path    []any  `json:"path,omitempty"`
}

// Handler returns an http.HandlerFunc that serves GraphQL queries against db.
func Handler(db *dgraph2.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("read body: %w", err))
			return
		}
		var req Request
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
			return
		}
		resp := Execute(r.Context(), db, &req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// Execute parses the GraphQL request, translates each top-level operation
// into DQL or a Mutate call, and returns a GraphQL-shaped response.
func Execute(ctx context.Context, db *dgraph2.DB, req *Request) *Response {
	if req == nil || strings.TrimSpace(req.Query) == "" {
		return &Response{Errors: []errorEntry{{Message: "empty query"}}}
	}

	doc, perr := parser.ParseQuery(&ast.Source{Input: req.Query})
	if perr != nil {
		return &Response{Errors: []errorEntry{{Message: perr.Message}}}
	}

	// Pick the operation. If --operation-name is supplied and matches one,
	// use it; otherwise pick the first.
	var op *ast.OperationDefinition
	for _, o := range doc.Operations {
		if req.OperationName == "" || o.Name == req.OperationName {
			op = o
			break
		}
	}
	if op == nil {
		return &Response{Errors: []errorEntry{{Message: "no matching operation"}}}
	}

	switch op.Operation {
	case ast.Query:
		return executeQuery(ctx, db, op, req.Variables)
	case ast.Mutation:
		return executeMutation(ctx, db, op, req.Variables)
	default:
		return &Response{Errors: []errorEntry{{
			Message: fmt.Sprintf("operation %q is not supported", op.Operation),
		}}}
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(&Response{
		Errors: []errorEntry{{Message: err.Error()}},
	})
}

// asMutation builds a *api.Mutation from raw RDF text.
func asMutation(rdf string) *api.Mutation {
	return &api.Mutation{SetNquads: []byte(rdf)}
}
