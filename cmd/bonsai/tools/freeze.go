/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package tools

import (
	stdflag "flag"
	"fmt"
	"os"

	"github.com/qiangli/bonsai/pkg/bonsai"
)

// FreezeMain is invoked from cmd/bonsai/main.go for `bonsai freeze <dir> -o <file>`.
// It compacts the source data dir and writes a single-file gzipped artifact.
//
// The DB at <dir> must not be in use by another bonsai-server process at
// the time of the freeze; Freeze opens it directly to flatten.
func FreezeMain() {
	fs := stdflag.NewFlagSet("bonsai freeze", stdflag.ExitOnError)
	out := fs.String("o", "", "output artifact (.bonsai); required")
	_ = fs.Parse(os.Args[1:])
	args := fs.Args()
	if len(args) < 1 || *out == "" {
		fmt.Fprintln(os.Stderr, "usage: bonsai freeze <data-dir> -o <out.bonsai>")
		os.Exit(2)
	}
	src := args[0]
	if err := bonsai.Freeze(src, *out); err != nil {
		fmt.Fprintf(os.Stderr, "freeze: %v\n", err)
		os.Exit(1)
	}
	if fi, err := os.Stat(*out); err == nil {
		fmt.Printf("frozen %s (%d bytes)\n", *out, fi.Size())
	}
}
