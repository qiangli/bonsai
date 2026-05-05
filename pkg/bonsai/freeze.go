/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Frozen-DB artifact format. Optimised for the build-once / query-many
 * workflow: an analyser writes a graph once, runs Freeze to produce a
 * single .bonsai file, ships it, and consumers OpenFrozen it as a
 * read-only DB. Avoids the operational tax of a writable Badger
 * directory in environments that only ever read.
 *
 * The artifact is a gzipped tar of the live data dir's `p/` Badger
 * subdirectory. Roundtrip is symmetric:
 *
 *   Freeze(srcDir, "graph.bonsai")        // pack
 *   db, _ := OpenFrozen("graph.bonsai")   // unpack to temp + open RO
 *   defer db.Close()                      // closes Badger + removes temp
 */

package bonsai

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Freeze closes a build-once DB into a single-file artifact at dstFile.
// srcDir must contain a `p/` subdirectory (the standard bonsai data
// layout). Freeze opens the DB, runs a final Flatten so on-disk size is
// tight, closes it, then tar-gzips the directory.
//
// Tightening note: callers who want maximum compression should set
// CompactOnClose on the open just before calling Freeze. Freeze itself
// only flattens; deeper rewrites are out of scope.
func Freeze(srcDir, dstFile string) error {
	pdir := filepath.Join(srcDir, "p")
	if fi, err := os.Stat(pdir); err != nil || !fi.IsDir() {
		return fmt.Errorf("Freeze: %s is not a bonsai data directory (no p/)", srcDir)
	}

	// Open and flatten so the artifact is as small as possible.
	db, err := Open(Options{Dir: srcDir, CompactOnClose: true})
	if err != nil {
		return fmt.Errorf("Freeze: open: %w", err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("Freeze: close: %w", err)
	}

	out, err := os.Create(dstFile)
	if err != nil {
		return fmt.Errorf("Freeze: create %s: %w", dstFile, err)
	}
	defer func() { _ = out.Close() }()

	gz := gzip.NewWriter(out)
	defer func() { _ = gz.Close() }()
	tw := tar.NewWriter(gz)
	defer func() { _ = tw.Close() }()

	return filepath.Walk(pdir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(pdir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			if _, err := io.Copy(tw, f); err != nil {
				return err
			}
		}
		return nil
	})
}

// OpenFrozen extracts a `.bonsai` artifact (produced by Freeze) into a
// temporary directory and opens it as a read-only DB. The temp dir is
// removed when Close is called. Multiple processes can OpenFrozen the
// same artifact concurrently; each gets its own temp copy.
//
// Memory floor for a frozen open is dramatically lower than a writable
// open because the value-log GC ticker, mutation lock, and posting
// in-memory cache all run lighter when the DB is read-only.
func OpenFrozen(path string) (*DB, error) {
	if path == "" {
		return nil, errors.New("OpenFrozen: path is required")
	}
	tmp, err := os.MkdirTemp("", "bonsai-frozen-*")
	if err != nil {
		return nil, fmt.Errorf("OpenFrozen: mkdir temp: %w", err)
	}
	pdir := filepath.Join(tmp, "p")
	if err := os.Mkdir(pdir, 0o755); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}

	src, err := os.Open(path)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("OpenFrozen: open %s: %w", path, err)
	}
	defer func() { _ = src.Close() }()
	gz, err := gzip.NewReader(src)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("OpenFrozen: gunzip: %w", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = os.RemoveAll(tmp)
			return nil, fmt.Errorf("OpenFrozen: tar read: %w", err)
		}
		out := filepath.Join(pdir, hdr.Name)
		// Path-traversal guard: the absolute path of `out` must still be
		// inside `pdir`. Defends against archive entries like
		// "../../etc/passwd".
		absPdir, _ := filepath.Abs(pdir)
		absOut, _ := filepath.Abs(out)
		rel, err := filepath.Rel(absPdir, absOut)
		if err != nil || rel == ".." || filepath.IsAbs(rel) ||
			(len(rel) >= 3 && rel[:3] == "../") {
			_ = os.RemoveAll(tmp)
			return nil, fmt.Errorf("OpenFrozen: malicious path %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(out, 0o755); err != nil {
				_ = os.RemoveAll(tmp)
				return nil, err
			}
		default:
			if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
				_ = os.RemoveAll(tmp)
				return nil, err
			}
			f, err := os.OpenFile(out, os.O_CREATE|os.O_WRONLY, 0o644)
			if err != nil {
				_ = os.RemoveAll(tmp)
				return nil, err
			}
			if _, err := io.Copy(f, tr); err != nil {
				_ = f.Close()
				_ = os.RemoveAll(tmp)
				return nil, err
			}
			_ = f.Close()
		}
	}

	db, err := Open(Options{Dir: tmp, ReadOnly: true})
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, fmt.Errorf("OpenFrozen: open extracted: %w", err)
	}
	db.frozenTemp = tmp
	return db, nil
}
