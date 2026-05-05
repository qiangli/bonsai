/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Local-only subcommands that don't talk to a server: `diff` (compare two
 * NTX exports), and (placeholder) any other future utility commands.
 */

package tools

import (
	"bufio"
	"fmt"
	"os"
)

// DiffMain is invoked from cmd/bonsai/main.go for `bonsai diff <a.ntx> <b.ntx>`.
// It reports the line-level differences between two NTX exports — they are
// already deterministically sorted by ExportTo("ntx", ...), so a plain
// line diff is meaningful.
//
// Output mirrors the unified-diff convention without context (just `+` for
// added, `-` for removed lines), which is enough for graph-snapshot review.
// Exit code: 0 if files match, 1 if they differ, 2 on read error. Mirrors
// /usr/bin/diff semantics so this command can drop into shell pipelines.
func DiffMain() {
	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bonsai diff <a.ntx> <b.ntx>")
		os.Exit(2)
	}
	a, err := readLines(args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "diff: %s: %v\n", args[0], err)
		os.Exit(2)
	}
	b, err := readLines(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "diff: %s: %v\n", args[1], err)
		os.Exit(2)
	}

	added, removed := lineDiff(a, b)
	if len(added) == 0 && len(removed) == 0 {
		// Files match.
		os.Exit(0)
	}
	for _, l := range removed {
		fmt.Println("-", l)
	}
	for _, l := range added {
		fmt.Println("+", l)
	}
	os.Exit(1)
}

// readLines slurps a file into a string slice. NTX files are typically
// well under a few hundred MB, so loading both into memory is fine.
func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []string
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 64*1024), 16*1024*1024) // allow large RDF lines
	for s.Scan() {
		out = append(out, s.Text())
	}
	return out, s.Err()
}

// lineDiff returns the set of lines added in b relative to a, and the set
// removed from a relative to b. NTX is sorted, so we walk both in step.
func lineDiff(a, b []string) (added, removed []string) {
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			i++
			j++
		case a[i] < b[j]:
			removed = append(removed, a[i])
			i++
		default:
			added = append(added, b[j])
			j++
		}
	}
	for ; i < len(a); i++ {
		removed = append(removed, a[i])
	}
	for ; j < len(b); j++ {
		added = append(added, b[j])
	}
	return added, removed
}
