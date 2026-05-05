/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Embedded single-page UI. Served by dgraph2-server at "/" via go:embed
 * so there's no build step, no Node, and no second deployment.
 */

package ui

import (
	"embed"
	"net/http"
)

//go:embed index.html
var assets embed.FS

// Handler serves the embedded UI. Mount at "/" or any prefix; the page
// uses absolute paths to /admin/schema, /admin/state, /query, /graphql,
// so it talks to whatever dgraph2-server is hosting it.
func Handler() http.Handler {
	// http.FileServerFS would do, but we want / to map to index.html and
	// keep the package boundary self-contained.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "" || path == "/" || path == "/ui" || path == "/ui/" {
			path = "index.html"
		} else {
			// Strip leading slash so the embed FS lookup matches.
			if path[0] == '/' {
				path = path[1:]
			}
		}
		data, err := assets.ReadFile(path)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		// Set a sensible Content-Type. Today there's only one file
		// (index.html); when we add CSS/JS this branches.
		switch {
		case len(path) > 5 && path[len(path)-5:] == ".html":
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
		case len(path) > 4 && path[len(path)-4:] == ".css":
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
		case len(path) > 3 && path[len(path)-3:] == ".js":
			w.Header().Set("Content-Type", "application/javascript")
		}
		_, _ = w.Write(data)
	})
}
