/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package graphql_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"
	"github.com/gorilla/websocket"

	"github.com/qiangli/bonsai/pkg/graphql"
)

// TestSubscriptionLifecycle wires a real HTTP server to
// graphql.SubscriptionHandler, opens a WebSocket as a client, runs a
// subscription, mutates the DB, and confirms the client receives an
// updated `next` frame.
func TestSubscriptionLifecycle(t *testing.T) {
	db := newDB(t)
	ctx := context.Background()
	if err := db.Alter(ctx, `
		name: string @index(exact) .
		type Person {
			name
		}
	`); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	if _, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:a <name> "Alice" .
		_:a <dgraph.type> "Person" .
	`)}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	srv := httptest.NewServer(graphql.SubscriptionHandler(db))
	defer srv.Close()

	wsURL, _ := url.Parse(srv.URL)
	wsURL.Scheme = "ws"

	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{"graphql-transport-ws"}
	conn, _, err := dialer.Dial(wsURL.String(), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	mustSend := func(m map[string]any) {
		if err := conn.WriteJSON(m); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	readMsg := func(d time.Duration) map[string]any {
		_ = conn.SetReadDeadline(time.Now().Add(d))
		var m map[string]any
		if err := conn.ReadJSON(&m); err != nil {
			t.Fatalf("read (timeout %s): %v", d, err)
		}
		return m
	}

	// Handshake.
	mustSend(map[string]any{"type": "connection_init"})
	if got := readMsg(2 * time.Second); got["type"] != "connection_ack" {
		t.Fatalf("connection_ack expected, got %+v", got)
	}

	// Subscribe.
	mustSend(map[string]any{
		"id":   "s1",
		"type": "subscribe",
		"payload": map[string]any{
			"query": `{ queryPerson { name } }`,
		},
	})

	// Initial frame: should contain Alice.
	first := readMsg(2 * time.Second)
	if first["type"] != "next" {
		t.Fatalf("first frame type = %v, want next: %+v", first["type"], first)
	}
	if !containsName(first, "Alice") {
		t.Fatalf("first frame missing Alice: %+v", first)
	}

	// Mutate to add Bob; the subscription should push an updated frame.
	if _, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
		_:b <name> "Bob" .
		_:b <dgraph.type> "Person" .
	`)}); err != nil {
		t.Fatalf("Mutate Bob: %v", err)
	}

	// Wait for a `next` that includes Bob — the poller is at 250ms so a
	// 3s deadline is comfortable.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m := readMsg(time.Until(deadline))
		if m["type"] == "next" && containsName(m, "Bob") {
			// Got the update.
			mustSend(map[string]any{"id": "s1", "type": "complete"})
			return
		}
	}
	t.Fatalf("never received update with Bob")
}

// containsName walks a `next` frame and reports whether the given name
// appears anywhere in the payload.
func containsName(m map[string]any, name string) bool {
	body, _ := json.Marshal(m)
	return strings.Contains(string(body), `"name":"`+name+`"`)
}

// TestSubscriptionConnectionAck confirms a basic init/ack handshake
// without any subscriptions still works (catches regressions in the
// readJSON loop).
func TestSubscriptionConnectionAck(t *testing.T) {
	db := newDB(t)
	srv := httptest.NewServer(graphql.SubscriptionHandler(db))
	defer srv.Close()

	wsURL, _ := url.Parse(srv.URL)
	wsURL.Scheme = "ws"
	dialer := *websocket.DefaultDialer
	dialer.Subprotocols = []string{"graphql-transport-ws"}
	conn, resp, err := dialer.Dial(wsURL.String(), http.Header{})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if resp.Header.Get("Sec-WebSocket-Protocol") != "graphql-transport-ws" {
		t.Fatalf("subprotocol negotiation failed: got %q",
			resp.Header.Get("Sec-WebSocket-Protocol"))
	}

	if err := conn.WriteJSON(map[string]any{"type": "connection_init"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var got map[string]any
	if err := conn.ReadJSON(&got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got["type"] != "connection_ack" {
		t.Fatalf("got %+v", got)
	}
}
