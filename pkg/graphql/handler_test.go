/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package graphql_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/dgraph2/pkg/dgraph2"
	"github.com/qiangli/dgraph2/pkg/graphql"
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
