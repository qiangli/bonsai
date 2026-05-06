/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package tools

import (
	"reflect"
	"testing"
)

// TestReorderFlags pins the argv-shuffling that lets users write
// `bonsai explore <path> --no-serve` (flags after positional). Stdlib
// flag stops at the first non-flag, so without the shuffle the bool
// flag is silently ignored.
func TestReorderFlags(t *testing.T) {
	valueFlags := map[string]bool{"o": true, "http": true, "grpc": true}
	for _, tc := range []struct {
		name string
		in   []string
		want []string
	}{
		{
			name: "flags first, no shuffle needed",
			in:   []string{"--no-serve", "/path"},
			want: []string{"--no-serve", "/path"},
		},
		{
			name: "bool flag after positional is moved before",
			in:   []string{"/path", "--no-serve"},
			want: []string{"--no-serve", "/path"},
		},
		{
			name: "value flag with space-separated value, after positional",
			in:   []string{"/path", "-o", "/data"},
			want: []string{"-o", "/data", "/path"},
		},
		{
			name: "equals form is a single token",
			in:   []string{"/path", "-o=/data"},
			want: []string{"-o=/data", "/path"},
		},
		{
			name: "mixed",
			in:   []string{"/path", "-f", "-http", ":9090", "--no-serve"},
			want: []string{"-f", "-http", ":9090", "--no-serve", "/path"},
		},
		{
			name: "no flags",
			in:   []string{"/path"},
			want: []string{"/path"},
		},
		{
			name: "lone dash is positional",
			in:   []string{"-", "--no-serve"},
			want: []string{"--no-serve", "-"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := reorderFlags(tc.in, valueFlags)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("reorderFlags(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
