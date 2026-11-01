/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package graphql

import (
	"bytes"
	"context"
	"testing"
	"time"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/bonsai/pkg/bonsai"
)

// TestSubscriptionMutationDuringInitialSend pins the window between the
// initial query and the first poll. The send callback is the exact point at
// which the WebSocket client can see Alice and immediately write Bob.
func TestSubscriptionMutationDuringInitialSend(t *testing.T) {
	db, err := bonsai.Open(bonsai.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
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
		t.Fatalf("seed Alice: %v", err)
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	frames := make(chan wsMessage, 3)
	sendErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		first := true
		runSubscription(subCtx, db, "s1", subPayload{Query: `{ queryPerson { name } }`}, func(m wsMessage) error {
			frames <- m
			if first && m.Type == "next" {
				first = false
				_, err := db.Mutate(ctx, &apiproto.Mutation{SetNquads: []byte(`
					_:b <name> "Bob" .
					_:b <dgraph.type> "Person" .
				`)})
				if err != nil {
					sendErr <- err
				}
				return err
			}
			return nil
		})
	}()

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for _, name := range []string{"Alice", "Bob"} {
		select {
		case m := <-frames:
			if m.Type != "next" || !bytes.Contains(m.Payload, []byte(`"name":"`+name+`"`)) {
				t.Fatalf("frame for %s: type=%q payload=%s", name, m.Type, m.Payload)
			}
		case err := <-sendErr:
			t.Fatalf("mutate Bob: %v", err)
		case <-deadline.C:
			t.Fatalf("no subscription frame containing %s within 3s", name)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscription did not stop after cancellation")
	}
}
