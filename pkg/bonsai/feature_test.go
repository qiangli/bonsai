/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Validation pass for DQL features that were ported from priorart in
 * worker/task.go but never explicitly tested in bonsai. Each test
 * documents whether the feature works end-to-end, fails with a known
 * error, or is genuinely broken and needs fixing.
 *
 * Run order: each test gets its own t.TempDir() so prior state is
 * isolated; process-global posting/schema/worker state is shared but
 * the active DB pointer is per-test.
 */

package bonsai_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/bonsai/pkg/bonsai"
)

func newDB(t *testing.T) *bonsai.DB {
	t.Helper()
	db, err := bonsai.Open(bonsai.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustAlter(t *testing.T, db *bonsai.DB, schema string) {
	t.Helper()
	if err := db.Alter(context.Background(), schema); err != nil {
		t.Fatalf("Alter: %v", err)
	}
}

func mustMutate(t *testing.T, db *bonsai.DB, rdf string) *apiproto.Response {
	t.Helper()
	resp, err := db.Mutate(context.Background(), &apiproto.Mutation{SetNquads: []byte(rdf)})
	if err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	return resp
}

func mustQuery(t *testing.T, db *bonsai.DB, q string) string {
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
	// Facet values must round-trip into the JSON output as
	// `<edge>|<facet-name>`. Earlier the test was defensive about this
	// (a t.Logf if missing); the engine actually emits them correctly.
	for _, want := range []string{`"friend|since":2020`, `"friend|weight":0.9`} {
		if !strings.Contains(got, want) {
			t.Errorf("@facets value missing %q in: %s", want, got)
		}
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

// Export → Import round-trip preserves both scalar values and uid edges.
// Earlier export only walked the head value of each posting list, so
// uid-list predicates silently dropped on the floor — round-tripping
// would lose every relationship in the graph.
func TestFeatureExportImportRoundtrip(t *testing.T) {
	src := newDB(t)
	mustAlter(t, src, `
		name:   string @index(exact) .
		friend: [uid]                 .
	`)
	mustMutate(t, src, `
		_:a <name>   "Alice" .
		_:b <name>   "Bob"   .
		_:c <name>   "Carol" .
		_:a <friend> _:b .
		_:a <friend> _:c .
		_:b <friend> _:c .
	`)

	// Export to RDF in memory.
	var buf bytes.Buffer
	if err := src.ExportTo(context.Background(), "rdf", &buf); err != nil {
		t.Fatalf("Export: %v", err)
	}
	dump := buf.String()
	// Sanity: every edge must appear as `<uid> <friend> <uid>` not as a
	// quoted literal. If the regression returns we'll see "friend" missing.
	if !strings.Contains(dump, "<friend> <0x") {
		t.Errorf("export dropped uid edges. dump:\n%s", dump)
	}

	// Restore into a fresh DB. We do NOT pre-apply the schema — the user
	// flow is "export from prod, import into dev" and they shouldn't have
	// to reconstruct the schema first. Apply a minimal schema then import.
	dst := newDB(t)
	mustAlter(t, dst, `
		name:   string @index(exact) .
		friend: [uid]                 .
	`)
	if _, err := bonsai.ImportStream(context.Background(), dst, "rdf",
		strings.NewReader(dump), 100); err != nil {
		t.Fatalf("ImportStream: %v", err)
	}

	got := mustQuery(t, dst, `{
		q(func: eq(name, "Alice")) { name friend { name } }
	}`)
	if !strings.Contains(got, `"name":"Alice"`) {
		t.Errorf("Alice missing after import: %s", got)
	}
	for _, want := range []string{"Bob", "Carol"} {
		if !strings.Contains(got, want) {
			t.Errorf("Alice's %s edge missing after import: %s", want, got)
		}
	}
}

// Auto-detect import: hand a tiny NetworkX node-link JSON to ImportStream
// without pre-applying any schema and confirm it (a) gets recognised,
// (b) generates a permissive schema on the fly, and (c) leaves a
// queryable graph behind.
func TestFeatureAutoDetectNetworkXImport(t *testing.T) {
	db := newDB(t)
	doc := []byte(`{
	  "directed": true,
	  "multigraph": false,
	  "graph": {},
	  "nodes": [
	    {"id": "a", "label": "Alice", "tags": ["person", "vip"]},
	    {"id": "b", "label": "Bob"},
	    {"id": "c", "label": "Carol"}
	  ],
	  "links": [
	    {"source": "a", "target": "b", "relation": "knows"},
	    {"source": "a", "target": "c", "relation": "knows"},
	    {"source": "b", "target": "c", "relation": "knows"}
	  ]
	}`)

	summary, err := bonsai.ImportStream(context.Background(), db, "json",
		bytes.NewReader(doc), 100)
	if err != nil {
		t.Fatalf("ImportStream: %v", err)
	}
	if summary.Detected != "networkx-node-link" {
		t.Errorf("Detected=%q, want networkx-node-link", summary.Detected)
	}
	if summary.Nquads == 0 {
		t.Errorf("zero nquads ingested: %+v", summary)
	}

	got := mustQuery(t, db, `{
		q(func: eq(gid, "a")) {
			gid
			label
			tags
			knows { gid label }
		}
	}`)
	for _, want := range []string{`"gid":"a"`, `"label":"Alice"`, `"label":"Bob"`, `"label":"Carol"`} {
		if !strings.Contains(got, want) {
			t.Errorf("auto-imported graph missing %s: %s", want, got)
		}
	}
}

// Freeze + OpenFrozen round-trips the build-once / query-many workflow:
// build a graph, freeze to a single artifact, open the artifact
// read-only, query it. Writes against the frozen DB error.
func TestFeatureFreezeRoundtrip(t *testing.T) {
	src := t.TempDir()
	artifact := src + "/graph.bonsai"
	ctx := context.Background()

	// Build phase.
	{
		db, err := bonsai.Open(bonsai.Options{Dir: src})
		if err != nil {
			t.Fatalf("Open build: %v", err)
		}
		mustAlter(t, db, "name: string @index(exact) .\nfriend: [uid] .\n")
		mustMutate(t, db, `
			_:a <name>   "Alice" .
			_:b <name>   "Bob"   .
			_:a <friend> _:b .
		`)
		_ = db.Close()
	}

	// Freeze.
	if err := bonsai.Freeze(src, artifact); err != nil {
		t.Fatalf("Freeze: %v", err)
	}
	fi, err := os.Stat(artifact)
	if err != nil {
		t.Fatalf("artifact stat: %v", err)
	}
	if fi.Size() == 0 {
		t.Errorf("frozen artifact is empty")
	}

	// Open frozen, verify reads work + writes are rejected.
	db, err := bonsai.OpenFrozen(artifact)
	if err != nil {
		t.Fatalf("OpenFrozen: %v", err)
	}
	defer db.Close()
	if !db.ReadOnly() {
		t.Errorf("frozen DB should be ReadOnly")
	}

	got := mustQuery(t, db, `{ q(func: has(name)) { name friend { name } } }`)
	for _, want := range []string{"Alice", "Bob"} {
		if !strings.Contains(got, want) {
			t.Errorf("frozen DB missing %s: %s", want, got)
		}
	}

	if _, err := db.Mutate(ctx, &apiproto.Mutation{
		SetNquads: []byte(`_:c <name> "Carol" .`),
	}); !errors.Is(err, bonsai.ErrReadOnly) {
		t.Errorf("Mutate on frozen DB: want ErrReadOnly, got %v", err)
	}
}

// Options.AutoSchema infers a permissive schema for unknown predicates
// at Mutate time so callers can write fast-evolving graphs without
// pre-declaring every edge.
func TestFeatureAutoSchema(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := bonsai.Open(bonsai.Options{Dir: dir, AutoSchema: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Mutate without any prior Alter — gid (string) and friend (uid edge)
	// should both succeed and have schema entries auto-inferred.
	if _, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:a <gid>    "alice"  .
		_:b <gid>    "bob"    .
		_:a <friend> _:b      .
	`)}); err != nil {
		t.Fatalf("Mutate (autoschema): %v", err)
	}
	got := mustQuery(t, db, `{ q(func: has(gid)) { gid friend { gid } } }`)
	if !strings.Contains(got, `"gid":"alice"`) || !strings.Contains(got, `"gid":"bob"`) {
		t.Errorf("autoschema lost data: %s", got)
	}
	if !strings.Contains(got, `"friend":[{"gid":"bob"}]`) {
		t.Errorf("autoschema didn't recognise uid edge: %s", got)
	}
}

// Without AutoSchema, an unknown predicate must still be rejected — the
// strict default is what schema-careful users rely on.
func TestFeatureAutoSchemaOffByDefault(t *testing.T) {
	db := newDB(t) // no AutoSchema
	_, err := db.Mutate(context.Background(), &apiproto.Mutation{
		SetNquads: []byte(`_:a <undeclared> "x" .`),
	})
	if err == nil || !strings.Contains(err.Error(), "no schema for predicate") {
		t.Errorf("strict default should reject unknown predicate, got %v", err)
	}
}

// Options.ReadOnly opens the DB without write access. Mutate / Alter /
// Drop must all return ErrReadOnly; Query / Get / ExportTo continue to
// work. Mirrors the build-once / query-many production deployment shape
// (the third party freezes a graph artifact and serves it RO).
func TestFeatureReadOnly(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Seed the DB with one node, then close.
	{
		db, err := bonsai.Open(bonsai.Options{Dir: dir})
		if err != nil {
			t.Fatalf("Open seed: %v", err)
		}
		mustAlter(t, db, "name: string @index(exact) .\n")
		mustMutate(t, db, `_:a <name> "Alice" .`)
		_ = db.Close()
	}

	// Reopen read-only. Writes are rejected, reads still work.
	db, err := bonsai.Open(bonsai.Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatalf("Open read-only: %v", err)
	}
	defer db.Close()
	if !db.ReadOnly() {
		t.Errorf("ReadOnly() should return true")
	}

	if err := db.Alter(ctx, "age: int .\n"); err == nil || !errors.Is(err, bonsai.ErrReadOnly) {
		t.Errorf("Alter on RO: want ErrReadOnly, got %v", err)
	}
	_, err = db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`_:b <name> "Bob" .`)})
	if err == nil || !errors.Is(err, bonsai.ErrReadOnly) {
		t.Errorf("Mutate on RO: want ErrReadOnly, got %v", err)
	}
	if err := db.DropAll(ctx); err == nil || !errors.Is(err, bonsai.ErrReadOnly) {
		t.Errorf("DropAll on RO: want ErrReadOnly, got %v", err)
	}

	// Reads must still see the seeded data.
	got := mustQuery(t, db, `{ q(func: has(name)) { name } }`)
	if !strings.Contains(got, "Alice") {
		t.Errorf("read on RO missing Alice: %s", got)
	}
	if strings.Contains(got, "Bob") {
		t.Errorf("read on RO leaked Bob (should have been rejected): %s", got)
	}
}

// NTX export must be byte-deterministic across runs of the same logical
// state — this is the property that makes `bonsai diff` meaningful and
// makes graph snapshots commit-able.
func TestFeatureExportNTXDeterministic(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()
	mustAlter(t, db, "name: string @index(exact) .\nfriend: [uid] .\n")
	mustMutate(t, db, `
		_:a <name>   "Alice" .
		_:b <name>   "Bob"   .
		_:c <name>   "Carol" .
		_:a <friend> _:b .
		_:b <friend> _:c .
		_:a <friend> _:c .
	`)

	var first, second bytes.Buffer
	if err := db.ExportTo(ctx, "ntx", &first); err != nil {
		t.Fatalf("export 1: %v", err)
	}
	if err := db.ExportTo(ctx, "ntx", &second); err != nil {
		t.Fatalf("export 2: %v", err)
	}
	if first.String() != second.String() {
		t.Errorf("ntx export not deterministic.\nfirst:\n%s\nsecond:\n%s",
			first.String(), second.String())
	}
	// Sanity: the dump should contain every name and the friend edges.
	for _, want := range []string{"Alice", "Bob", "Carol", "<friend>"} {
		if !strings.Contains(first.String(), want) {
			t.Errorf("ntx missing %q\n%s", want, first.String())
		}
	}
	// Lines must be sorted by subject UID hex (zero-padded so 0x2 < 0xa).
	// The export sorts on a 20-hex-digit UID prefix, so even at large UID
	// counts the order is right.
	lines := strings.Split(strings.TrimRight(first.String(), "\n"), "\n")
	for i := 1; i < len(lines); i++ {
		if lines[i-1] > lines[i] {
			t.Errorf("ntx not sorted at line %d: %q vs %q",
				i, lines[i-1], lines[i])
		}
	}
}

// A failing edge inside a multi-edge Mutate must leave the DB untouched —
// no partial commit. The unknown-predicate rejection is what we exercise
// here because it's the easiest failure to trigger; the same atomicity
// applies to type-conversion failures and the like.
func TestFeatureMutateAtomicOnPartialFailure(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()
	mustAlter(t, db, "name: string @index(exact) .\n") // `unknown_pred` deliberately omitted

	// Batch has two valid edges plus one with an unknown predicate. The
	// unknown predicate trips runMutation's schema check and aborts the
	// whole batch — no Alice/Bob should appear in the DB.
	_, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:a <name>          "Alice" .
		_:b <name>          "Bob" .
		_:a <unknown_pred>  "should-fail" .
	`)})
	if err == nil {
		t.Fatalf("expected Mutate to error on unknown predicate")
	}
	if !strings.Contains(err.Error(), "no schema for predicate") {
		t.Errorf("unexpected error wording: %v", err)
	}

	// Confirm the DB is empty — no Alice / Bob from the failed batch.
	got := mustQuery(t, db, `{ q(func: has(name)) { name } }`)
	if strings.Contains(got, "Alice") || strings.Contains(got, "Bob") {
		t.Errorf("partial commit leaked: %s", got)
	}
	if !strings.Contains(got, `"q":[]`) && !strings.Contains(got, "{}") {
		t.Errorf("expected empty query result, got: %s", got)
	}
}

// QueryAsOf reads at a past timestamp. We anchor the snapshot at the
// first write's commit timestamp (returned by Mutate); reading there
// should see Alice but not Bob, who is inserted afterwards.
func TestFeaturePITQueryAsOf(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\n")
	aliceResp := mustMutate(t, db, `_:a <name> "Alice" .`)
	tsBefore := aliceResp.GetTxn().GetCommitTs()
	if tsBefore == 0 {
		t.Fatalf("Alice's mutation has zero commit_ts: %+v", aliceResp.Txn)
	}

	// Second write, after the snapshot.
	mustMutate(t, db, `_:b <name> "Bob" .`)

	// Current view sees both.
	now := mustQuery(t, db, `{ q(func: has(name)) { name } }`)
	if !strings.Contains(now, "Alice") || !strings.Contains(now, "Bob") {
		t.Fatalf("current view missing entries: %s", now)
	}

	// Read at Alice's commit ts: only Alice should be visible.
	asOf, err := db.QueryAsOf(context.Background(), tsBefore, `{ q(func: has(name)) { name } }`)
	if err != nil {
		t.Fatalf("QueryAsOf: %v", err)
	}
	body := string(asOf.Json)
	if !strings.Contains(body, "Alice") {
		t.Errorf("PIT view missing Alice: %s", body)
	}
	if strings.Contains(body, "Bob") {
		t.Errorf("PIT view leaked Bob (snapshot ts=%d): %s", tsBefore, body)
	}

	// Reading past the current high-water mark errors.
	if _, err := db.QueryAsOf(context.Background(), 1<<60, `{ q(func: has(name)) { name } }`); err == nil {
		t.Errorf("expected QueryAsOf at future ts to error")
	}
}

// PIT restore from a backup chain — full backup → mutate → incremental →
// mutate again → incremental again → restore-up-to-after-first-incremental
// into a fresh DB and assert only the first two batches survive.
func TestFeaturePITRestoreFromManifest(t *testing.T) {
	src := newDB(t)
	bdir := t.TempDir()
	mustAlter(t, src, "name: string @index(exact) .\n")
	ctx := context.Background()

	// Round 1: Alice. Full backup.
	mustMutate(t, src, `_:a <name> "Alice" .`)
	full, err := src.BackupTo(ctx, bonsai.BackupOptions{Dir: bdir, Type: bonsai.BackupFull})
	if err != nil {
		t.Fatalf("full backup: %v", err)
	}

	// Round 2: Bob. Incremental.
	mustMutate(t, src, `_:b <name> "Bob" .`)
	incr1, err := src.BackupTo(ctx, bonsai.BackupOptions{Dir: bdir, Type: bonsai.BackupIncremental})
	if err != nil {
		t.Fatalf("incr1 backup: %v", err)
	}

	// Round 3: Carol. Incremental we'll deliberately roll past during PIT.
	mustMutate(t, src, `_:c <name> "Carol" .`)
	if _, err := src.BackupTo(ctx, bonsai.BackupOptions{Dir: bdir, Type: bonsai.BackupIncremental}); err != nil {
		t.Fatalf("incr2 backup: %v", err)
	}

	// PIT restore: bound at incr1's ReadTs — full + incr1 should apply,
	// incr2 should be skipped.
	dst := newDB(t)
	if err := dst.RestoreFromManifestWithOptions(ctx, bdir, bonsai.RestoreOptions{
		UntilTs: incr1.ReadTs,
	}); err != nil {
		t.Fatalf("PIT restore: %v", err)
	}

	got := mustQuery(t, dst, `{ q(func: has(name)) { name } }`)
	for _, want := range []string{"Alice", "Bob"} {
		if !strings.Contains(got, want) {
			t.Errorf("PIT @ incr1 missing %s: %s", want, got)
		}
	}
	if strings.Contains(got, "Carol") {
		t.Errorf("PIT @ incr1 leaked Carol (which is in incr2): %s", got)
	}

	// PIT bound older than the full backup itself is rejected.
	dst2 := newDB(t)
	if err := dst2.RestoreFromManifestWithOptions(ctx, bdir, bonsai.RestoreOptions{
		UntilTs: full.ReadTs - 1,
	}); err == nil {
		t.Errorf("expected error when UntilTs precedes the full backup")
	}
}

// within(geo, polygon) — points stored, query asks for points inside a
// polygon. Reuses the same NYC/LA fixture as TestFeatureGeoNear.
func TestFeatureGeoWithin(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\nloc: geo @index(geo) .\n")
	mustMutate(t, db, `
		_:a <name> "TimesSquare" .
		_:a <loc>  "{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7580]}"^^<geo:geojson> .
		_:b <name> "EmpireState" .
		_:b <loc>  "{\"type\":\"Point\",\"coordinates\":[-73.9857,40.7484]}"^^<geo:geojson> .
		_:c <name> "LAX" .
		_:c <loc>  "{\"type\":\"Point\",\"coordinates\":[-118.4081,33.9425]}"^^<geo:geojson> .
	`)
	// Manhattan-ish bounding box (-74.05,-73.90 lng × 40.70,40.80 lat).
	got := mustQuery(t, db, `{
		q(func: within(loc, [[[-74.05,40.70],[-73.90,40.70],[-73.90,40.80],[-74.05,40.80],[-74.05,40.70]]])) {
			name
		}
	}`)
	if !strings.Contains(got, "TimesSquare") || !strings.Contains(got, "EmpireState") {
		t.Errorf("within() missed NYC points: %s", got)
	}
	if strings.Contains(got, "LAX") {
		t.Errorf("within() returned LA point: %s", got)
	}
}

// contains(geo, point) — stored polygons, query asks which polygons
// contain a given point. Stores two non-overlapping polygons (a NYC box
// and an LA box) and asks for the polygon that contains a Manhattan
// coordinate.
func TestFeatureGeoContains(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\narea: geo @index(geo) .\n")
	mustMutate(t, db, `
		_:nyc <name> "ManhattanBox" .
		_:nyc <area> "{\"type\":\"Polygon\",\"coordinates\":[[[-74.05,40.70],[-73.90,40.70],[-73.90,40.80],[-74.05,40.80],[-74.05,40.70]]]}"^^<geo:geojson> .
		_:la  <name> "LABox" .
		_:la  <area> "{\"type\":\"Polygon\",\"coordinates\":[[[-118.50,33.90],[-118.30,33.90],[-118.30,34.10],[-118.50,34.10],[-118.50,33.90]]]}"^^<geo:geojson> .
	`)
	// Times Square coordinates — should land in the Manhattan box.
	got := mustQuery(t, db, `{
		q(func: contains(area, [-73.9857, 40.7580])) { name }
	}`)
	if !strings.Contains(got, "ManhattanBox") {
		t.Errorf("contains() missed enclosing polygon: %s", got)
	}
	if strings.Contains(got, "LABox") {
		t.Errorf("contains() returned non-enclosing polygon: %s", got)
	}
}

// intersects(geo, polygon) — stored polygons, query asks which intersect
// a given polygon (overlap or touch). One stored box overlaps the query
// rectangle, the other is far away.
func TestFeatureGeoIntersects(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, "name: string @index(exact) .\narea: geo @index(geo) .\n")
	mustMutate(t, db, `
		_:nyc <name> "ManhattanBox" .
		_:nyc <area> "{\"type\":\"Polygon\",\"coordinates\":[[[-74.05,40.70],[-73.90,40.70],[-73.90,40.80],[-74.05,40.80],[-74.05,40.70]]]}"^^<geo:geojson> .
		_:la  <name> "LABox" .
		_:la  <area> "{\"type\":\"Polygon\",\"coordinates\":[[[-118.50,33.90],[-118.30,33.90],[-118.30,34.10],[-118.50,34.10],[-118.50,33.90]]]}"^^<geo:geojson> .
	`)
	// Query rectangle straddling -74.00 long, 40.75 lat — overlaps the
	// Manhattan box but is nowhere near LA.
	got := mustQuery(t, db, `{
		q(func: intersects(area, [[[-74.00,40.75],[-73.95,40.75],[-73.95,40.78],[-74.00,40.78],[-74.00,40.75]]])) {
			name
		}
	}`)
	if !strings.Contains(got, "ManhattanBox") {
		t.Errorf("intersects() missed overlapping polygon: %s", got)
	}
	if strings.Contains(got, "LABox") {
		t.Errorf("intersects() returned non-overlapping polygon: %s", got)
	}
}

// similar_to() over an HNSW vector index. Inserts three 2D points, asks
// for the two nearest to (0,0), expects the close pair back and the far
// one excluded.
func TestFeatureVectorSimilarTo(t *testing.T) {
	db := newDB(t)
	mustAlter(t, db, `
		name:      string         @index(exact) .
		embedding: float32vector  @index(hnsw(metric: "euclidean")) .
	`)
	mustMutate(t, db, `
		_:a <name>      "Origin"  .
		_:a <embedding> "[0.0, 0.0]"^^<float32vector> .
		_:b <name>      "Near"    .
		_:b <embedding> "[0.1, 0.1]"^^<float32vector> .
		_:c <name>      "Far"     .
		_:c <embedding> "[10.0, 10.0]"^^<float32vector> .
	`)
	got := mustQuery(t, db, `{
		q(func: similar_to(embedding, 2, "[0.0, 0.0]")) { name }
	}`)
	if !strings.Contains(got, "Origin") || !strings.Contains(got, "Near") {
		t.Errorf("similar_to missed nearby points: %s", got)
	}
	if strings.Contains(got, "Far") {
		t.Errorf("similar_to returned far point: %s", got)
	}
}
