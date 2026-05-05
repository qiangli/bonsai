/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package graphql_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/bonsai/pkg/bonsai"
	"github.com/qiangli/bonsai/pkg/graphql"
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

// TestGraphQLAddAndQuery exercises the add<Type> + query<Type> + get<Type>
// happy path against a minimal Person schema.
func TestGraphQLAddAndQuery(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()
	if err := db.Alter(ctx, `
		name: string @index(exact) .
		age:  int    .
		type Person {
			name
			age
		}
	`); err != nil {
		t.Fatalf("Alter: %v", err)
	}

	add := graphql.Execute(ctx, db, &graphql.Request{
		Query: `mutation { addPerson(input: {name: "Alice", age: 30}) { uid } }`,
	})
	if len(add.Errors) > 0 {
		t.Fatalf("addPerson errors: %+v", add.Errors)
	}
	addBody, _ := json.Marshal(add.Data["addPerson"])
	if !strings.Contains(string(addBody), `"uid":"0x`) {
		t.Errorf("addPerson missing uid: %s", addBody)
	}

	q := graphql.Execute(ctx, db, &graphql.Request{
		Query: `{ queryPerson { name age } }`,
	})
	if len(q.Errors) > 0 {
		t.Fatalf("queryPerson errors: %+v", q.Errors)
	}
	body, _ := json.Marshal(q.Data["queryPerson"])
	if !strings.Contains(string(body), `"name":"Alice"`) {
		t.Errorf("queryPerson missing Alice: %s", body)
	}
	if !strings.Contains(string(body), `"age":30`) {
		t.Errorf("queryPerson missing age: %s", body)
	}

	g := graphql.Execute(ctx, db, &graphql.Request{
		Query: `{ getPerson(id: "0x1") { name } }`,
	})
	if len(g.Errors) > 0 {
		t.Fatalf("getPerson errors: %+v", g.Errors)
	}
	gbody, _ := json.Marshal(g.Data["getPerson"])
	if !strings.Contains(string(gbody), `"name":"Alice"`) {
		t.Errorf("getPerson missing Alice: %s", gbody)
	}
}

// TestGraphQLNestedSelection exercises uid-edge expansion through the
// translator.
func TestGraphQLNestedSelection(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()
	if err := db.Alter(ctx, `
		name:   string @index(exact) .
		friend: [uid] .
		type Person {
			name
			friend
		}
	`); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	// Use raw RDF for the friend edge — the GraphQL translator only handles
	// scalar input fields for now.
	if _, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:a <name> "Alice" .
		_:b <name> "Bob"   .
		_:a <friend> _:b .
		_:a <dgraph.type> "Person" .
		_:b <dgraph.type> "Person" .
	`)}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}

	r := graphql.Execute(ctx, db, &graphql.Request{
		Query: `{ queryPerson { name friend { name } } }`,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("nested errors: %+v", r.Errors)
	}
	body, _ := json.Marshal(r.Data["queryPerson"])
	if !strings.Contains(string(body), `"friend":[{"name":"Bob"}]`) {
		t.Errorf("nested friend expansion missing: %s", body)
	}
}

// TestGraphQLUpdateAndDelete exercises the update<Type> and delete<Type>
// mutation paths via filter → DQL UID resolution.
func TestGraphQLUpdateAndDelete(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()
	if err := db.Alter(ctx, `
		name: string @index(exact) .
		age:  int    .
		type Person {
			name
			age
		}
	`); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	for _, name := range []string{"Alice", "Bob", "Carol"} {
		r := graphql.Execute(ctx, db, &graphql.Request{
			Query: `mutation ($n: String!) { addPerson(input: {name: $n, age: 30}) { uid } }`,
			Variables: map[string]any{"n": name},
		})
		if len(r.Errors) > 0 {
			t.Fatalf("addPerson(%s): %+v", name, r.Errors)
		}
	}

	// Update via filter.eq: bump Alice's age to 31.
	upd := graphql.Execute(ctx, db, &graphql.Request{
		Query: `mutation {
			updatePerson(
				filter: {eq: {predicate: "name", value: "Alice"}}
				set:    {age: 31}
			) { uids count }
		}`,
	})
	if len(upd.Errors) > 0 {
		t.Fatalf("updatePerson: %+v", upd.Errors)
	}
	updBody, _ := json.Marshal(upd.Data["updatePerson"])
	if !strings.Contains(string(updBody), `"count":1`) {
		t.Errorf("update count != 1: %s", updBody)
	}

	// Verify the update landed.
	q := graphql.Execute(ctx, db, &graphql.Request{
		Query: `{ queryPerson { name age } }`,
	})
	body, _ := json.Marshal(q.Data["queryPerson"])
	if !strings.Contains(string(body), `"name":"Alice","age":31`) &&
		!strings.Contains(string(body), `"age":31,"name":"Alice"`) {
		t.Errorf("Alice's age not updated to 31: %s", body)
	}

	// Delete Bob.
	del := graphql.Execute(ctx, db, &graphql.Request{
		Query: `mutation {
			deletePerson(filter: {eq: {predicate: "name", value: "Bob"}}) { count }
		}`,
	})
	if len(del.Errors) > 0 {
		t.Fatalf("deletePerson: %+v", del.Errors)
	}

	q2 := graphql.Execute(ctx, db, &graphql.Request{
		Query: `{ queryPerson { name } }`,
	})
	body2, _ := json.Marshal(q2.Data["queryPerson"])
	if strings.Contains(string(body2), `"Bob"`) {
		t.Errorf("Bob still present after delete: %s", body2)
	}
	for _, want := range []string{`"Alice"`, `"Carol"`} {
		if !strings.Contains(string(body2), want) {
			t.Errorf("delete blew away %s: %s", want, body2)
		}
	}
}

// TestGraphQLSyntaxError ensures parse failures come back as GraphQL-shape
// errors rather than a panic or 500.
func TestGraphQLSyntaxError(t *testing.T) {
	db := newDB(t)
	r := graphql.Execute(context.Background(), db, &graphql.Request{
		Query: `not a graphql query`,
	})
	if len(r.Errors) == 0 {
		t.Fatalf("expected error, got %+v", r)
	}
}
