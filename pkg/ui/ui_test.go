/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package ui_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/qiangli/dgraph2/pkg/ui"
)

// TestHandlerServesIndex confirms `/` returns the embedded HTML page
// with the right Content-Type and the page actually contains the wiring
// the JS expects (so renames don't silently break the UI).
func TestHandlerServesIndex(t *testing.T) {
	h := ui.Handler()
	for _, path := range []string{"/", "/ui", "/ui/", "/index.html"} {
		req := httptest.NewRequest("GET", path, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Errorf("GET %s: got %d", path, w.Code)
			continue
		}
		ct := w.Header().Get("Content-Type")
		if !strings.HasPrefix(ct, "text/html") {
			t.Errorf("GET %s: Content-Type %q", path, ct)
		}
		body := w.Body.String()
		for _, want := range []string{
			"dgraph2 Explorer",
			`id="query-text"`,
			`id="schema-list"`,
			`/admin/schema`,
			`/query`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("GET %s body missing %q", path, want)
			}
		}
	}
}

// TestHandlerNotFound confirms unknown paths return 404 rather than
// dumping the index page (so tools probing /favicon.ico or /robots.txt
// don't get a fake 200).
func TestHandlerNotFound(t *testing.T) {
	h := ui.Handler()
	req := httptest.NewRequest("GET", "/no-such-asset.css", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Errorf("expected 404, got %d", w.Code)
	}
}
