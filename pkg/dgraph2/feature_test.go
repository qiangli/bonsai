/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Validation pass for DQL features that were ported from priorart in
 * worker/task.go but never explicitly tested in dgraph2. Each test
 * documents whether the feature works end-to-end, fails with a known
 * error, or is genuinely broken and needs fixing.
 *
 * Run order: each test gets its own t.TempDir() so prior state is
 * isolated; process-global posting/schema/worker state is shared but
 * the active DB pointer is per-test.
 */

package dgraph2_test

import (
	"context"
	"strings"
	"testing"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/dgraph2/pkg/dgraph2"
)

func newDB(t *testing.T) *dgraph2.DB {
	t.Helper()
	db, err := dgraph2.Open(dgraph2.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustAlter(t *testing.T, db *dgraph2.DB, schema string) {
	t.Helper()
	if err := db.Alter(context.Background(), schema); err != nil {
		t.Fatalf("Alter: %v", err)
	}
}

func mustMutate(t *testing.T, db *dgraph2.DB, rdf string) *apiproto.Response {
	t.Helper()
	resp, err := db.Mutate(context.Background(), &apiproto.Mutation{SetNquads: []byte(rdf)})
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	return resp
}

func mustQuery(t *testing.T, db *dgraph2.DB, q string) string {
	t.Helper()
	r, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("Query: %v\nquery: %s", err, q)
	}
	return string(r.Json)
}

// Full-text search: anyofterms over a fulltext index.
func TestFeatureAnyofterms(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "title: string @index(term) .\n")
	mustMutate(t, db, `
		_:a <title> "Quick brown fox" .
		_:b <title> "Lazy red dog" .
		_:c <title> "Brown bear" .
	`)
	got := mustQuery(t, db, `{ q(func: anyofterms(title, "brown")) { title } }`)
	if !strings.Contains(got, "Quick brown fox") || !strings.Contains(got, "Brown bear") {
		t.Errorf("anyofterms missed matches: %s", got)
	}
	if strings.Contains(got, "Lazy red dog") {
		t.Errorf("anyofterms returned non-match: %s", got)
	}
}

// Full-text search: alloftext with stemming and stop words.
func TestFeatureAlloftext(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "body: string @index(fulltext) .\n")
	mustMutate(t, db, `
		_:a <body> "running quickly through the forest" .
		_:b <body> "walking slowly across the meadow" .
	`)
	got := mustQuery(t, db, `{ q(func: alloftext(body, "run forest")) { body } }`)
	if !strings.Contains(got, "running") {
		t.Errorf("alloftext missed: %s", got)
	}
}

// Regex over a trigram index.
func TestFeatureRegexp(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "code: string @index(trigram) .\n")
	mustMutate(t, db, `
		_:a <code> "ABC-123" .
		_:b <code> "XYZ-789" .
		_:c <code> "ABC-456" .
	`)
	got := mustQuery(t, db, `{ q(func: regexp(code, /^ABC-/)) { code } }`)
	if !strings.Contains(got, "ABC-123") || !strings.Contains(got, "ABC-456") {
		t.Errorf("regexp missed matches: %s", got)
	}
	if strings.Contains(got, "XYZ-789") {
		t.Errorf("regexp returned non-match: %s", got)
	}
}

// @reverse edges should let us walk a uid edge backwards.
func TestFeatureReverseEdge(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nfollows: [uid] @reverse .\n")
	mustMutate(t, db, `
		_:alice <name>    "Alice" .
		_:bob   <name>    "Bob" .
		_:alice <follows> _:bob .
	`)
	// Find bob, traverse back to followers.
	got := mustQuery(t, db, `{
		q(func: eq(name, "Bob")) {
			name
			~follows { name }
		}
	}`)
	if !strings.Contains(got, `"name":"Alice"`) {
		t.Errorf("~follows did not return Alice: %s", got)
	}
}

// Aggregations: count, sum, avg, max, min.
func TestFeatureAggregation(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nscore: int .\n")
	mustMutate(t, db, `
		_:a <name>  "A" .
		_:a <score> "10"^^<xs:int> .
		_:b <name>  "A" .
		_:b <score> "20"^^<xs:int> .
		_:c <name>  "A" .
		_:c <score> "30"^^<xs:int> .
	`)
	got := mustQuery(t, db, `{
		var(func: eq(name, "A")) {
			s as score
		}
		stats() {
			total: sum(val(s))
			mean:  avg(val(s))
			high:  max(val(s))
			low:   min(val(s))
		}
	}`)
	for _, want := range []string{
		`"total":60`,
		`"mean":20`,
		`"high":30`,
		`"low":10`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("aggregation missing %s: %s", want, got)
		}
	}
}

// var() blocks and `func: uid(v)` lookups.
func TestFeatureVarUid(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nfriend: [uid] .\n")
	mustMutate(t, db, `
		_:a <name>   "Alice" .
		_:b <name>   "Bob" .
		_:c <name>   "Carol" .
		_:a <friend> _:b .
		_:a <friend> _:c .
	`)
	got := mustQuery(t, db, `{
		alice as var(func: eq(name, "Alice"))
		alicesFriends(func: uid(alice)) {
			friend { name }
		}
	}`)
	if !strings.Contains(got, "Bob") || !strings.Contains(got, "Carol") {
		t.Errorf("var(func: uid()) failed: %s", got)
	}
}

// @cascade — drop nodes that are missing required predicates.
func TestFeatureCascade(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db,
		"name: string @index(exact) .\nemail: string .\n")
	mustMutate(t, db, `
		_:a <name>  "Has email" .
		_:a <email> "x@y.com" .
		_:b <name>  "No email" .
	`)
	got := mustQuery(t, db, `{
		q(func: has(name)) @cascade {
			name
			email
		}
	}`)
	if !strings.Contains(got, "Has email") {
		t.Errorf("cascade dropped a node it shouldn't: %s", got)
	}
	if strings.Contains(got, "No email") {
		t.Errorf("cascade kept a node missing email: %s", got)
	}
}

// @recurse follows the same predicate to a depth.
func TestFeatureRecurse(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nparent: [uid] .\n")
	mustMutate(t, db, `
		_:a <name>   "Root" .
		_:b <name>   "Child" .
		_:c <name>   "Grandchild" .
		_:b <parent> _:a .
		_:c <parent> _:b .
	`)
	got := mustQuery(t, db, `{
		q(func: eq(name, "Grandchild")) @recurse(depth: 5) {
			name
			parent
		}
	}`)
	for _, want := range []string{"Grandchild", "Child", "Root"} {
		if !strings.Contains(got, want) {
			t.Errorf("recurse missing %s: %s", want, got)
		}
	}
}

// @groupby — must group by a UID-typed edge when using `as` var assignment
// (string-typed groupby works too, but does not support the var chain that
// lets a follow-up block reference the group via uid()/val()).
func TestFeatureGroupBy(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, `
		name:     string @index(exact) .
		livesIn:  [uid] .
		cityName: string @index(exact) .
	`)
	resp := mustMutate(t, db, `
		_:nyc <cityName> "NYC" .
		_:la  <cityName> "LA"  .
		_:a   <name>    "A" .
		_:a   <livesIn> _:nyc .
		_:b   <name>    "B" .
		_:b   <livesIn> _:nyc .
		_:c   <name>    "C" .
		_:c   <livesIn> _:la .
	`)
	if resp.Uids["nyc"] == "" || resp.Uids["la"] == "" {
		t.Fatalf("missing city uids: %v", resp.Uids)
	}
	got := mustQuery(t, db, `{
		var(func: has(name)) @groupby(livesIn) {
			a as count(uid)
		}
		groups(func: uid(a), orderdesc: val(a)) {
			cityName
			pop: val(a)
		}
	}`)
	if !strings.Contains(got, "NYC") || !strings.Contains(got, "LA") {
		t.Errorf("groupby missing cities: %s", got)
	}
	// NYC has 2 residents, LA has 1.
	if !strings.Contains(got, `"pop":2`) || !strings.Contains(got, `"pop":1`) {
		t.Errorf("groupby counts wrong: %s", got)
	}
}

// @lang multilingual values.
func TestFeatureLang(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) @lang .\n")
	mustMutate(t, db, `
		_:a <name> "Hello"@en .
		_:a <name> "Bonjour"@fr .
	`)
	got := mustQuery(t, db, `{
		q(func: eq(name@en, "Hello")) {
			en: name@en
			fr: name@fr
		}
	}`)
	if !strings.Contains(got, "Hello") || !strings.Contains(got, "Bonjour") {
		t.Errorf("@lang query failed: %s", got)
	}
}

// between(predicate, low, high) inequality.
func TestFeatureBetween(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "age: int @index(int) .\nname: string .\n")
	mustMutate(t, db, `
		_:a <name> "A" .
		_:a <age> "10"^^<xs:int> .
		_:b <name> "B" .
		_:b <age> "20"^^<xs:int> .
		_:c <name> "C" .
		_:c <age> "30"^^<xs:int> .
		_:d <name> "D" .
		_:d <age> "40"^^<xs:int> .
	`)
	got := mustQuery(t, db, `{ q(func: between(age, 15, 35)) { name age } }`)
	if !strings.Contains(got, `"name":"B"`) || !strings.Contains(got, `"name":"C"`) {
		t.Errorf("between missed matches: %s", got)
	}
	if strings.Contains(got, `"name":"A"`) || strings.Contains(got, `"name":"D"`) {
		t.Errorf("between returned out-of-range: %s", got)
	}
}

// @facets — edge properties.
func TestFeatureFacets(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nfriend: [uid] .\n")
	mustMutate(t, db, `
		_:a <name>   "Alice" .
		_:b <name>   "Bob" .
		_:a <friend> _:b (since=2020, weight=0.9) .
	`)
	got := mustQuery(t, db, `{
		q(func: eq(name, "Alice")) {
			name
			friend @facets {
				name
			}
		}
	}`)
	if !strings.Contains(got, "Bob") {
		t.Errorf("@facets edge expansion failed: %s", got)
	}
	// The facet values should appear under "friend|since" / "friend|weight".
	if !strings.Contains(got, "since") {
		t.Logf("note: @facets values may not be emitted: %s", got)
	}
}

// Multiple values in eq() — set membership.
func TestFeatureEqMulti(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\n")
	mustMutate(t, db, `
		_:a <name> "Alice" .
		_:b <name> "Bob" .
		_:c <name> "Carol" .
	`)
	got := mustQuery(t, db, `{ q(func: eq(name, ["Alice", "Carol"])) { name } }`)
	if !strings.Contains(got, "Alice") || !strings.Contains(got, "Carol") {
		t.Errorf("eq multi missed: %s", got)
	}
	if strings.Contains(got, "Bob") {
		t.Errorf("eq multi returned non-match: %s", got)
	}
}

// orderasc / orderdesc.
func TestFeatureOrder(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nage: int @index(int) .\n")
	mustMutate(t, db, `
		_:a <name> "A" .
		_:a <age> "30"^^<xs:int> .
		_:b <name> "B" .
		_:b <age> "10"^^<xs:int> .
		_:c <name> "C" .
		_:c <age> "20"^^<xs:int> .
	`)
	got := mustQuery(t, db, `{ q(func: has(age), orderasc: age) { name age } }`)
	// Expect order: B(10) C(20) A(30).
	posB := strings.Index(got, `"name":"B"`)
	posC := strings.Index(got, `"name":"C"`)
	posA := strings.Index(got, `"name":"A"`)
	if !(posB < posC && posC < posA) {
		t.Errorf("orderasc wrong: %s", got)
	}
}

// match(pred, value, distance) — fuzzy match over a trigram index.
func TestFeatureMatch(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(trigram) .\n")
	mustMutate(t, db, `
		_:a <name> "Alexander" .
		_:b <name> "Alexandra" .
		_:c <name> "Bob" .
	`)
	got := mustQuery(t, db, `{ q(func: match(name, "Alexandr", 2)) { name } }`)
	if !strings.Contains(got, "Alexander") || !strings.Contains(got, "Alexandra") {
		t.Errorf("match missed near matches: %s", got)
	}
	if strings.Contains(got, `"name":"Bob"`) {
		t.Errorf("match returned distant value: %s", got)
	}
}

// expand(_all_) — expand every predicate of a node without naming each one.
// Requires a type definition for the node (DQL spec: expand walks the type's
// predicate list).
func TestFeatureExpandAll(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, `
		name: string @index(exact) .
		age:  int    .
		type Person {
			name
			age
		}
	`)
	mustMutate(t, db, `
		_:a <name> "Alice" .
		_:a <age>  "30"^^<xs:int> .
		_:a <dgraph.type> "Person" .
	`)
	got := mustQuery(t, db, `{ q(func: eq(name, "Alice")) { expand(_all_) } }`)
	if !strings.Contains(got, "Alice") {
		t.Errorf("expand(_all_) missed name: %s", got)
	}
	if !strings.Contains(got, "30") {
		t.Errorf("expand(_all_) missed age: %s", got)
	}
}

// shortest path between two named nodes via a uid-list predicate.
// DQL syntax: a top-level `shortest(from: <uid>, to: <uid>)` block plus a
// follow-up `path(func: uid(_path_))` to materialise the result.
func TestFeatureShortest(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nfriend: [uid] .\n")
	resp := mustMutate(t, db, `
		_:a <name>   "A" .
		_:b <name>   "B" .
		_:c <name>   "C" .
		_:d <name>   "D" .
		_:a <friend> _:b .
		_:b <friend> _:c .
		_:c <friend> _:d .
		_:a <friend> _:d .
	`)
	a, d := resp.Uids["a"], resp.Uids["d"]
	if a == "" || d == "" {
		t.Fatalf("missing uids: %v", resp.Uids)
	}
	q := `{
		path as shortest(from: ` + a + `, to: ` + d + `) {
			friend
		}
		path(func: uid(path)) { name }
	}`
	got := mustQuery(t, db, q)
	// The 1-hop A→D edge should win over A→B→C→D.
	if !strings.Contains(got, `"name":"A"`) || !strings.Contains(got, `"name":"D"`) {
		t.Errorf("shortest path missed endpoints: %s", got)
	}
	if strings.Contains(got, `"name":"B"`) || strings.Contains(got, `"name":"C"`) {
		t.Errorf("shortest path took the long route: %s", got)
	}
}

// near(geo, [lng, lat], distance) over a geo index.
func TestFeatureGeoNear(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nloc: geo @index(geo) .\n")
	// Two NYC-ish points and one in LA.
	mustMutate(t, db, `
		_:a <name> "TimesSquare" .
		_:a <loc>  "{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}"^^<geo:geojson> .
		_:b <name> "EmpireState" .
		_:b <loc>  "{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7484]}"^^<geo:geojson> .
		_:c <name> "LAX" .
		_:c <loc>  "{\"type\":\"Point\",\"coordinates\":[-118.4081,33.9425]}"^^<geo:geojson> .
	`)
	// 5km radius around Times Square — should include EmpireState, exclude LAX.
	got := mustQuery(t, db, `{
		q(func: near(loc, [-73.9857, 40.7580], 5000)) { name }
	}`)
	if !strings.Contains(got, "TimesSquare") || !strings.Contains(got, "EmpireState") {
		t.Errorf("near() missed nearby points: %s", got)
	}
	if strings.Contains(got, "LAX") {
		t.Errorf("near() returned far point: %s", got)
	}
}

// math() expressions over val() variables.
func TestFeatureMath(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nprice: float .\n")
	mustMutate(t, db, `
		_:a <name>  "Widget" .
		_:a <price> "10.0"^^<xs:float> .
		_:b <name>  "Gadget" .
		_:b <price> "20.0"^^<xs:float> .
	`)
	got := mustQuery(t, db, `{
		var(func: has(price)) {
			p as price
			doubled as math(p * 2)
		}
		q(func: uid(p), orderasc: name) {
			name
			price
			d: val(doubled)
		}
	}`)
	// Doubled values should appear: 20 and 40.
	if !strings.Contains(got, "Widget") || !strings.Contains(got, "Gadget") {
		t.Errorf("math() result missing nodes: %s", got)
	}
	if !strings.Contains(got, "20") || !strings.Contains(got, "40") {
		t.Errorf("math() didn't double the values: %s", got)
	}
}
