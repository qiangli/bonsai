/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/dgraph-io/gqlparser/v2/ast"

	"github.com/qiangli/dgraph2/pkg/dgraph2"
)

// executeQuery walks each top-level field, translates it to DQL, runs it
// via db.Query, and merges the results into the response.
func executeQuery(ctx context.Context, db *dgraph2.DB, op *ast.OperationDefinition, vars map[string]any) *Response {
	resp := &Response{Data: map[string]any{}}
	for _, sel := range op.SelectionSet {
		f, ok := sel.(*ast.Field)
		if !ok {
			resp.Errors = append(resp.Errors, errorEntry{
				Message: "fragments are not supported",
			})
			continue
		}
		dql, err := buildQueryDQL(f, vars)
		if err != nil {
			resp.Errors = append(resp.Errors, errorEntry{Message: err.Error(), Path: []any{f.Alias}})
			continue
		}
		out, err := db.Query(ctx, dql)
		if err != nil {
			resp.Errors = append(resp.Errors, errorEntry{Message: err.Error(), Path: []any{f.Alias}})
			continue
		}
		// db.Query returns `{"q": [...]}`; lift the array under the GraphQL
		// field's alias so the caller sees `{"queryPerson": [...]}`.
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(out.Json, &raw); err != nil {
			resp.Errors = append(resp.Errors, errorEntry{Message: err.Error(), Path: []any{f.Alias}})
			continue
		}
		var lifted any
		if r, ok := raw["q"]; ok {
			_ = json.Unmarshal(r, &lifted)
		}
		resp.Data[f.Alias] = lifted
	}
	return resp
}

// buildQueryDQL turns a GraphQL field like `queryPerson { name }` or
// `getPerson(id: "0x1") { name }` into a DQL query string.
func buildQueryDQL(f *ast.Field, vars map[string]any) (string, error) {
	var rootFunc string
	switch {
	case strings.HasPrefix(f.Name, "query"):
		typ := strings.TrimPrefix(f.Name, "query")
		if typ == "" {
			return "", fmt.Errorf("queryX requires a type name (e.g. queryPerson)")
		}
		rootFunc = fmt.Sprintf("type(%s)", typ)
	case strings.HasPrefix(f.Name, "get"):
		typ := strings.TrimPrefix(f.Name, "get")
		if typ == "" {
			return "", fmt.Errorf("getX requires a type name (e.g. getPerson)")
		}
		uid, err := requireStringArg(f, "id", vars)
		if err != nil {
			return "", err
		}
		// Validate the uid looks like a hex literal — DQL requires 0x….
		if !strings.HasPrefix(uid, "0x") {
			return "", fmt.Errorf("getX id must be a 0x-prefixed uid (got %q)", uid)
		}
		rootFunc = fmt.Sprintf("uid(%s)", uid)
		_ = typ // type is informational; DQL filters by uid alone
	default:
		return "", fmt.Errorf("unsupported top-level field %q (use query<Type> or get<Type>)", f.Name)
	}

	body, err := buildSelectionSet(f.SelectionSet, vars, 1)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("{ q(func: %s) %s }", rootFunc, body), nil
}

// buildSelectionSet renders a GraphQL selection set as a DQL `{ … }` block.
// Nested fields with sub-selections become nested DQL blocks; leaf fields
// become bare predicate names. The `uid` GraphQL field maps to DQL `uid`.
func buildSelectionSet(set ast.SelectionSet, vars map[string]any, depth int) (string, error) {
	if len(set) == 0 {
		return "", nil
	}
	indent := strings.Repeat("  ", depth)
	var b strings.Builder
	b.WriteString("{\n")
	for _, sel := range set {
		f, ok := sel.(*ast.Field)
		if !ok {
			return "", fmt.Errorf("fragments are not supported in selection set")
		}
		b.WriteString(indent)
		// `uid` and `dgraph.type` need no rewriting; they're DQL predicates.
		predicate := f.Name
		if f.Alias != "" && f.Alias != f.Name {
			b.WriteString(f.Alias)
			b.WriteString(": ")
		}
		b.WriteString(predicate)
		if len(f.SelectionSet) > 0 {
			sub, err := buildSelectionSet(f.SelectionSet, vars, depth+1)
			if err != nil {
				return "", err
			}
			b.WriteString(" ")
			b.WriteString(sub)
		}
		b.WriteString("\n")
	}
	b.WriteString(strings.Repeat("  ", depth-1))
	b.WriteString("}")
	return b.String(), nil
}

// executeMutation handles `addX(input: {...})` mutations.
func executeMutation(ctx context.Context, db *dgraph2.DB, op *ast.OperationDefinition, vars map[string]any) *Response {
	resp := &Response{Data: map[string]any{}}
	for _, sel := range op.SelectionSet {
		f, ok := sel.(*ast.Field)
		if !ok {
			resp.Errors = append(resp.Errors, errorEntry{
				Message: "fragments are not supported",
			})
			continue
		}
		var (
			out map[string]any
			err error
		)
		switch {
		case strings.HasPrefix(f.Name, "add"):
			out, err = executeAdd(ctx, db, f, vars)
		case strings.HasPrefix(f.Name, "update"):
			out, err = executeUpdate(ctx, db, f, vars)
		case strings.HasPrefix(f.Name, "delete"):
			out, err = executeDelete(ctx, db, f, vars)
		default:
			err = fmt.Errorf("unsupported mutation %q (use add<Type> / update<Type> / delete<Type>)", f.Name)
		}
		if err != nil {
			resp.Errors = append(resp.Errors, errorEntry{
				Message: err.Error(), Path: []any{f.Alias},
			})
			continue
		}
		resp.Data[f.Alias] = out
	}
	return resp
}

// executeAdd builds RDF from the `input` argument and applies it.
func executeAdd(ctx context.Context, db *dgraph2.DB, f *ast.Field, vars map[string]any) (map[string]any, error) {
	typ := strings.TrimPrefix(f.Name, "add")
	if typ == "" {
		return nil, fmt.Errorf("addX requires a type name (e.g. addPerson)")
	}
	inputArg := f.Arguments.ForName("input")
	if inputArg == nil {
		return nil, fmt.Errorf("%s: missing input argument", f.Name)
	}
	val, err := evalValue(inputArg.Value, vars)
	if err != nil {
		return nil, err
	}
	obj, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.input must be an object, got %T", f.Name, val)
	}

	var rdf strings.Builder
	for k, v := range obj {
		fmt.Fprintf(&rdf, "_:new <%s> %s .\n", k, encodeRDFValue(v))
	}
	fmt.Fprintf(&rdf, "_:new <dgraph.type> \"%s\" .\n", typ)

	resp, err := db.Mutate(ctx, asMutation(rdf.String()))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name, err)
	}
	return map[string]any{"uid": resp.Uids["new"]}, nil
}

// executeUpdate applies `update<Type>(filter: {...}, set: {k: v, ...},
// remove: {k: v, ...}) { uids }` against db.Upsert.
//
// Supported filter shapes:
//
//	{filter: {id: "0x1"}}                                    single-uid lookup
//	{filter: {ids: ["0x1", "0x2"]}}                          uid set
//	{filter: {has: "name"}}                                  has(predicate)
//	{filter: {eq: {predicate: "name", value: "Alice"}}}      eq(predicate, value)
//
// We resolve the filter to a DQL query, then issue a Mutate that uses uid(v).
func executeUpdate(ctx context.Context, db *dgraph2.DB, f *ast.Field, vars map[string]any) (map[string]any, error) {
	typ := strings.TrimPrefix(f.Name, "update")
	if typ == "" {
		return nil, fmt.Errorf("updateX requires a type name (e.g. updatePerson)")
	}
	filterArg := f.Arguments.ForName("filter")
	if filterArg == nil {
		return nil, fmt.Errorf("%s: missing filter argument", f.Name)
	}
	filterVal, err := evalValue(filterArg.Value, vars)
	if err != nil {
		return nil, err
	}
	filterObj, ok := filterVal.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.filter must be an object", f.Name)
	}
	rootFn, err := filterToDQL(filterObj)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name, err)
	}

	// Build the upsert query that binds the matched uids to var `v`.
	queryDQL := fmt.Sprintf("query { v as var(func: %s) }", rootFn)

	var setRDF, delRDF strings.Builder
	if a := f.Arguments.ForName("set"); a != nil {
		val, err := evalValue(a.Value, vars)
		if err != nil {
			return nil, err
		}
		obj, ok := val.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s.set must be an object", f.Name)
		}
		for k, v := range obj {
			fmt.Fprintf(&setRDF, "uid(v) <%s> %s .\n", k, encodeRDFValue(v))
		}
	}
	if a := f.Arguments.ForName("remove"); a != nil {
		val, err := evalValue(a.Value, vars)
		if err != nil {
			return nil, err
		}
		obj, ok := val.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s.remove must be an object", f.Name)
		}
		for k, v := range obj {
			fmt.Fprintf(&delRDF, "uid(v) <%s> %s .\n", k, encodeRDFValue(v))
		}
	}
	if setRDF.Len() == 0 && delRDF.Len() == 0 {
		return nil, fmt.Errorf("%s: at least one of set / remove is required", f.Name)
	}

	mut := newApiMutation()
	mut.SetNquads = []byte(setRDF.String())
	mut.DelNquads = []byte(delRDF.String())
	if _, err := db.Upsert(ctx, queryDQL, mut); err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name, err)
	}

	// Return the resolved uids by re-querying — keeps the response shape
	// consistent with `delete<Type>` and lets the caller see what changed.
	return queryFilterUids(ctx, db, rootFn)
}

// executeDelete applies `delete<Type>(filter: {...}) { uids count }` by
// resolving the filter to UIDs and then issuing a per-predicate wildcard
// delete for every field in the type's schema definition. Upstream's
// `<uid> * * .` form would do the same in one go, but dgraph2's worker
// requires a known predicate per edge, so we expand explicitly.
func executeDelete(ctx context.Context, db *dgraph2.DB, f *ast.Field, vars map[string]any) (map[string]any, error) {
	typeName := strings.TrimPrefix(f.Name, "delete")
	if typeName == "" {
		return nil, fmt.Errorf("deleteX requires a type name (e.g. deletePerson)")
	}
	filterArg := f.Arguments.ForName("filter")
	if filterArg == nil {
		return nil, fmt.Errorf("%s: missing filter argument", f.Name)
	}
	filterVal, err := evalValue(filterArg.Value, vars)
	if err != nil {
		return nil, err
	}
	filterObj, ok := filterVal.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s.filter must be an object", f.Name)
	}
	rootFn, err := filterToDQL(filterObj)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name, err)
	}

	resolved, err := queryFilterUids(ctx, db, rootFn)
	if err != nil {
		return nil, err
	}
	uids, _ := resolved["uids"].([]string)
	if len(uids) == 0 {
		return resolved, nil
	}

	// Discover the type's predicate list from schema state. If the type is
	// unknown we still drop dgraph.type so subsequent queryX no longer
	// matches; callers will see lingering scalar values, which is the same
	// behaviour as upstream when a type isn't defined.
	preds := typePredicates(typeName)
	preds = append(preds, "dgraph.type")

	var rdf strings.Builder
	for _, u := range uids {
		for _, p := range preds {
			fmt.Fprintf(&rdf, "<%s> <%s> * .\n", u, p)
		}
	}
	mut := newApiMutation()
	mut.DelNquads = []byte(rdf.String())
	if _, err := db.Mutate(ctx, mut); err != nil {
		return nil, fmt.Errorf("%s: %w", f.Name, err)
	}
	return resolved, nil
}

// filterToDQL turns a simple filter map into a DQL root-function string.
func filterToDQL(f map[string]any) (string, error) {
	if v, ok := f["id"]; ok {
		s, ok := v.(string)
		if !ok || !strings.HasPrefix(s, "0x") {
			return "", fmt.Errorf("filter.id must be a 0x-prefixed string")
		}
		return fmt.Sprintf("uid(%s)", s), nil
	}
	if v, ok := f["ids"]; ok {
		list, ok := v.([]any)
		if !ok {
			return "", fmt.Errorf("filter.ids must be a list")
		}
		parts := make([]string, 0, len(list))
		for _, x := range list {
			s, ok := x.(string)
			if !ok {
				return "", fmt.Errorf("filter.ids entries must be strings")
			}
			parts = append(parts, s)
		}
		return fmt.Sprintf("uid(%s)", strings.Join(parts, ", ")), nil
	}
	if v, ok := f["has"]; ok {
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("filter.has must be a predicate name string")
		}
		return fmt.Sprintf("has(%s)", s), nil
	}
	if v, ok := f["eq"]; ok {
		obj, ok := v.(map[string]any)
		if !ok {
			return "", fmt.Errorf("filter.eq must be {predicate, value}")
		}
		pred, _ := obj["predicate"].(string)
		val := obj["value"]
		if pred == "" {
			return "", fmt.Errorf("filter.eq.predicate is required")
		}
		return fmt.Sprintf("eq(%s, %s)", pred, encodeRDFValue(val)), nil
	}
	return "", fmt.Errorf("unsupported filter; need id / ids / has / eq")
}

// queryFilterUids resolves a root-function to a list of UID strings via DQL.
func queryFilterUids(ctx context.Context, db *dgraph2.DB, rootFn string) (map[string]any, error) {
	resp, err := db.Query(ctx, fmt.Sprintf("{ q(func: %s) { uid } }", rootFn))
	if err != nil {
		return nil, err
	}
	var raw struct {
		Q []struct {
			UID string `json:"uid"`
		} `json:"q"`
	}
	if err := json.Unmarshal(resp.Json, &raw); err != nil {
		return nil, err
	}
	uids := make([]string, 0, len(raw.Q))
	for _, e := range raw.Q {
		uids = append(uids, e.UID)
	}
	return map[string]any{"uids": uids, "count": len(uids)}, nil
}

// typePredicates returns the list of bare predicate names declared on the
// given DQL type. Strips namespace prefixes from each field. Returns an
// empty slice if the type is unknown.
func typePredicates(typeName string) []string {
	t, ok := dgraphSchemaType(typeName)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(t.Fields))
	for _, fld := range t.Fields {
		_, attr := splitNamespacedAttr(fld.Predicate)
		out = append(out, attr)
	}
	return out
}

// requireStringArg pulls a named argument off a field as a string,
// resolving variables and inline values.
func requireStringArg(f *ast.Field, name string, vars map[string]any) (string, error) {
	a := f.Arguments.ForName(name)
	if a == nil {
		return "", fmt.Errorf("%s: missing %s argument", f.Name, name)
	}
	v, err := evalValue(a.Value, vars)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s.%s must be a string, got %T", f.Name, name, v)
	}
	return s, nil
}

// evalValue resolves a gqlparser ast.Value against the provided variables.
func evalValue(v *ast.Value, vars map[string]any) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch v.Kind {
	case ast.Variable:
		return vars[v.Raw], nil
	case ast.IntValue:
		n, err := strconv.ParseInt(v.Raw, 10, 64)
		return n, err
	case ast.FloatValue:
		n, err := strconv.ParseFloat(v.Raw, 64)
		return n, err
	case ast.StringValue, ast.BlockValue, ast.EnumValue:
		return v.Raw, nil
	case ast.BooleanValue:
		return v.Raw == "true", nil
	case ast.NullValue:
		return nil, nil
	case ast.ListValue:
		out := make([]any, 0, len(v.Children))
		for _, c := range v.Children {
			x, err := evalValue(c.Value, vars)
			if err != nil {
				return nil, err
			}
			out = append(out, x)
		}
		return out, nil
	case ast.ObjectValue:
		out := map[string]any{}
		for _, c := range v.Children {
			x, err := evalValue(c.Value, vars)
			if err != nil {
				return nil, err
			}
			out[c.Name] = x
		}
		return out, nil
	}
	return nil, fmt.Errorf("unhandled value kind %v", v.Kind)
}

// encodeRDFValue formats a Go value as an RDF object literal.
func encodeRDFValue(v any) string {
	switch x := v.(type) {
	case string:
		return strconv.Quote(x)
	case bool:
		return fmt.Sprintf("%q^^<xs:boolean>", strconv.FormatBool(x))
	case int, int32, int64:
		return fmt.Sprintf("%q^^<xs:int>", fmt.Sprintf("%d", x))
	case float32, float64:
		return fmt.Sprintf("%q^^<xs:float>", fmt.Sprintf("%v", x))
	case nil:
		return `""`
	default:
		// Fallback: JSON-encode and wrap as a string literal.
		b, _ := json.Marshal(x)
		return strconv.Quote(string(b))
	}
}
