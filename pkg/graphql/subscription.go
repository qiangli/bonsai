/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * GraphQL subscriptions over WebSocket using the graphql-transport-ws
 * subprotocol (the modern one — see graphql-ws npm package, not the
 * legacy subscriptions-transport-ws).
 *
 * Reactivity model: there is no live-query infrastructure under the hood.
 * Each active subscription polls db.MutationTick() at a short interval; if
 * the tick has advanced since the last poll, the GraphQL query is
 * re-evaluated and a `next` message is sent only when the result hash
 * differs from the previous send. A wall-clock fallback every few seconds
 * catches edge cases (e.g. a tick we somehow missed during reconnects).
 *
 * Tradeoffs vs upstream:
 *   - latency: ~poll interval (default 250ms after a write, 5s idle)
 *   - load: each subscription costs one query re-evaluation per tick
 *   - filter pushdown: none — the whole query re-runs on every change
 *
 * For low-rate, low-fanout scenarios (tens of subscribers, infrequent
 * writes) this is fine. Anything heavier would need real change tracking.
 */

package graphql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/qiangli/dgraph2/pkg/dgraph2"
)

const (
	// pollInterval is how often each active subscription checks the
	// mutation tick. After a tick advance we re-eval and push if changed.
	pollInterval = 250 * time.Millisecond
	// maxIdle forces a re-eval even when the tick hasn't advanced, in case
	// we missed a notification during reconnects or under contention.
	maxIdle = 5 * time.Second
)

// SubscriptionHandler returns an http.HandlerFunc that upgrades to a
// WebSocket and serves graphql-transport-ws subscriptions backed by db.
func SubscriptionHandler(db *dgraph2.DB) http.HandlerFunc {
	upgrader := websocket.Upgrader{
		Subprotocols: []string{"graphql-transport-ws"},
		// Allow connections from any origin — the operator surface is
		// already gated by the surrounding deployment (mTLS, network).
		CheckOrigin: func(*http.Request) bool { return true },
	}
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return // Upgrade already wrote an error response
		}
		serveWS(r.Context(), db, conn)
	}
}

// wsMessage is the on-the-wire envelope for both directions.
type wsMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// subPayload is the payload for type=subscribe (client → server).
type subPayload struct {
	Query         string         `json:"query"`
	OperationName string         `json:"operationName,omitempty"`
	Variables     map[string]any `json:"variables,omitempty"`
}

// serveWS handles one WebSocket connection's lifecycle: connection_init,
// subscribe / complete, and graceful close.
func serveWS(parentCtx context.Context, db *dgraph2.DB, conn *websocket.Conn) {
	defer func() { _ = conn.Close() }()

	// Coordinate writes from multiple subscription goroutines.
	var writeMu sync.Mutex
	writeJSON := func(m wsMessage) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(m)
	}

	// Track active subscriptions by id so `complete` can cancel them.
	type subEntry struct {
		cancel context.CancelFunc
	}
	subs := map[string]*subEntry{}
	var subsMu sync.Mutex

	// Read loop runs synchronously; subscription goroutines spawn off it.
	for {
		var msg wsMessage
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}
		switch msg.Type {
		case "connection_init":
			_ = writeJSON(wsMessage{Type: "connection_ack"})

		case "ping":
			_ = writeJSON(wsMessage{Type: "pong"})

		case "subscribe":
			var p subPayload
			if err := json.Unmarshal(msg.Payload, &p); err != nil {
				_ = writeJSON(wsMessage{ID: msg.ID, Type: "error",
					Payload: jsonRaw([]errorEntry{{Message: err.Error()}})})
				continue
			}
			ctx, cancel := context.WithCancel(parentCtx)
			subsMu.Lock()
			if old := subs[msg.ID]; old != nil {
				old.cancel()
			}
			subs[msg.ID] = &subEntry{cancel: cancel}
			subsMu.Unlock()
			go func(id string, payload subPayload, ctx context.Context, cancel context.CancelFunc) {
				defer func() {
					subsMu.Lock()
					delete(subs, id)
					subsMu.Unlock()
					cancel()
				}()
				runSubscription(ctx, db, id, payload, writeJSON)
			}(msg.ID, p, ctx, cancel)

		case "complete":
			subsMu.Lock()
			if e := subs[msg.ID]; e != nil {
				e.cancel()
			}
			subsMu.Unlock()

		default:
			// Unknown message types are ignored — the protocol allows
			// servers to be liberal in what they accept.
		}
	}

	// Connection closing: tear down every active subscription.
	subsMu.Lock()
	for _, e := range subs {
		e.cancel()
	}
	subsMu.Unlock()
}

// runSubscription is the per-subscription loop. It re-evaluates the query
// whenever db.MutationTick advances or maxIdle elapses, and pushes a
// `next` message only when the result hash changes from the prior send.
func runSubscription(
	ctx context.Context,
	db *dgraph2.DB,
	id string,
	p subPayload,
	send func(wsMessage) error,
) {
	defer func() { _ = send(wsMessage{ID: id, Type: "complete"}) }()

	pushResult := func(resp *Response) error {
		body, err := json.Marshal(resp)
		if err != nil {
			return err
		}
		return send(wsMessage{ID: id, Type: "next", Payload: body})
	}

	// Initial evaluation: every subscription should fire once with the
	// current state so clients see existing data immediately.
	prev, prevHash := evalAndHash(ctx, db, &p)
	if err := pushResult(prev); err != nil {
		return
	}

	lastTick := db.MutationTick()
	lastEval := time.Now()
	t := time.NewTicker(pollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		curTick := db.MutationTick()
		if curTick == lastTick && time.Since(lastEval) < maxIdle {
			continue
		}
		resp, h := evalAndHash(ctx, db, &p)
		lastTick = curTick
		lastEval = time.Now()
		if h == prevHash {
			continue
		}
		prevHash = h
		if err := pushResult(resp); err != nil {
			return
		}
	}
}

// evalAndHash runs Execute against the request and returns both the
// response and a stable hex hash of its serialised form.
func evalAndHash(ctx context.Context, db *dgraph2.DB, p *subPayload) (*Response, string) {
	resp := Execute(ctx, db, &Request{
		Query:         p.Query,
		OperationName: p.OperationName,
		Variables:     p.Variables,
	})
	body, _ := json.Marshal(resp)
	sum := sha256.Sum256(body)
	return resp, hex.EncodeToString(sum[:])
}

func jsonRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(fmt.Sprintf(`{"error":%q}`, err.Error()))
	}
	return b
}
