/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Multi-file backup format compatible with upstream Dgraph's `dgraph restore`.
 *
 * Layout produced in <Dir>:
 *   manifest.json                                       master manifest (array of per-backup entries)
 *   dgraph.<unix-ts>/r<ReadTs>-g1.backup                snappy-compressed KVList stream
 *   dgraph.<unix-ts>/manifest.json                      per-backup manifest (single entry, redundant for compatibility)
 *
 * Each `*.backup` file is a sequence of `[uint64 size LE][proto.Marshal(KVList)]`
 * chunks, all wrapped in s2 (snappy) compression. This matches
 * priorart/dgraph/worker/backup.go writeKVList + s2.NewWriter framing.
 *
 * Group ID is hardcoded to 1 (single node). Encryption is not produced;
 * the single-node fork keeps EncryptionKey reserved.
 */

package bonsai

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	badgerpb "github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/klauspost/compress/s2"
	"google.golang.org/protobuf/proto"

	"github.com/qiangli/bonsai/posting"
	"github.com/qiangli/bonsai/protos/pb"
	"github.com/qiangli/bonsai/schema"
	"github.com/qiangli/bonsai/worker"
)

// manifestVersion matches upstream's x.ManifestVersion. Upstream readers
// validate this field to ensure forward/back compatibility.
const manifestVersion = 2105

// BackupType is "full" or "incremental".
type BackupType string

const (
	BackupFull        BackupType = "full"
	BackupIncremental BackupType = "incremental"
)

// BackupOptions configures BackupTo. Defaults: Type=full, Compression=snappy.
type BackupOptions struct {
	// Dir is the destination root. The function appends a per-run
	// `dgraph.<unix-ts>` subdirectory under it.
	Dir string
	// Type is "full" or "incremental". When "incremental", Dir must already
	// hold a manifest.json from a previous full backup; SinceTs and BackupId
	// are taken from the latest manifest in the chain.
	Type BackupType
}

// Manifest is the per-backup manifest written into manifest.json. Field
// names and JSON tags are wire-compatible with upstream's
// worker.ManifestBase + worker.Manifest, minus encryption + drop-ops which
// bonsai does not produce.
type Manifest struct {
	Type        string              `json:"type"`
	SinceTs     uint64              `json:"since"`
	ReadTs      uint64              `json:"read_ts"`
	BackupID    string              `json:"backup_id"`
	BackupNum   uint64              `json:"backup_num"`
	Version     int                 `json:"version"`
	Path        string              `json:"path"`
	Encrypted   bool                `json:"encrypted"`
	Compression string              `json:"compression"`
	Groups      map[uint32][]string `json:"groups"`
	// DropOperations is always empty for bonsai (cluster-only field).
	DropOperations []any `json:"drop_operations"`
}

// MasterManifest is the top-level manifest.json file in the backup root.
type MasterManifest struct {
	Manifests []*Manifest `json:"manifests"`
}

// BackupTo produces an upstream-compatible multi-file backup under opts.Dir.
// On success, the directory contains an updated manifest.json plus a new
// per-backup subdirectory with the snappy-framed KVList stream.
func (d *DB) BackupTo(ctx context.Context, opts BackupOptions) (man *Manifest, err error) {
	defer d.auditDeferred("BackupTo", ctx, map[string]any{
		"dir":  opts.Dir,
		"type": string(opts.Type),
	}, &err)()
	if opts.Dir == "" {
		return nil, fmt.Errorf("BackupTo: Dir is required")
	}
	if opts.Type == "" {
		opts.Type = BackupFull
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("BackupTo: mkdir %s: %w", opts.Dir, err)
	}

	master, err := readMasterManifest(opts.Dir)
	if err != nil {
		return nil, err
	}

	readTs := d.tsCount.Load()
	if w := worker.CurrentTs(); w > readTs {
		readTs = w
	}

	man = &Manifest{
		Type:           string(opts.Type),
		ReadTs:         readTs,
		Version:        manifestVersion,
		Compression:    "snappy",
		Groups:         map[uint32][]string{1: schemaPredicates()},
		DropOperations: []any{},
	}

	switch opts.Type {
	case BackupFull:
		man.SinceTs = 0
		man.BackupNum = 1
		bid, err := newBackupID()
		if err != nil {
			return nil, err
		}
		man.BackupID = bid
	case BackupIncremental:
		if len(master.Manifests) == 0 {
			return nil, fmt.Errorf("BackupTo: incremental requested but %s has no prior manifest", opts.Dir)
		}
		latest := master.Manifests[len(master.Manifests)-1]
		man.SinceTs = latest.ReadTs
		man.BackupID = latest.BackupID
		man.BackupNum = latest.BackupNum + 1
		if readTs <= latest.ReadTs {
			return nil, fmt.Errorf("BackupTo: ReadTs %d not newer than latest %d", readTs, latest.ReadTs)
		}
	default:
		return nil, fmt.Errorf("BackupTo: unknown type %q", opts.Type)
	}

	subdir := fmt.Sprintf("dgraph.%d", time.Now().UnixMilli())
	man.Path = subdir
	if err := os.MkdirAll(filepath.Join(opts.Dir, subdir), 0o755); err != nil {
		return nil, fmt.Errorf("BackupTo: mkdir subdir: %w", err)
	}

	backupFile := filepath.Join(opts.Dir, subdir, fmt.Sprintf("r%d-g1.backup", readTs))
	if err := d.writeBackupFile(ctx, backupFile, readTs, man.SinceTs); err != nil {
		return nil, err
	}

	// Write the per-backup manifest (single entry) inside the subdir, then
	// the updated master manifest at the root. Upstream restore reads the
	// root manifest first.
	master.Manifests = append(master.Manifests, man)
	if err := writeManifestAtomic(filepath.Join(opts.Dir, subdir, "manifest.json"),
		&MasterManifest{Manifests: []*Manifest{man}}); err != nil {
		return nil, err
	}
	if err := writeManifestAtomic(filepath.Join(opts.Dir, "manifest.json"), master); err != nil {
		return nil, err
	}
	return man, nil
}

// writeBackupFile streams the entire keyspace at readTs into a snappy-framed
// KVList sequence. SinceTs > 0 emits an incremental delta.
func (d *DB) writeBackupFile(ctx context.Context, path string, readTs, sinceTs uint64) (err error) {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
	}()

	cw := s2.NewWriter(f)
	defer func() {
		if cerr := cw.Close(); err == nil {
			err = cerr
		}
	}()

	stream := d.pstore.NewStreamAt(readTs)
	stream.LogPrefix = "bonsai.BackupTo"
	stream.SinceTs = sinceTs
	stream.Send = func(buf *z.Buffer) error {
		list, lerr := badger.BufferToKVList(buf)
		if lerr != nil {
			return lerr
		}
		return writeKVListChunk(cw, list)
	}
	if err := stream.Orchestrate(ctx); err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	return nil
}

// writeKVListChunk writes one KVList preceded by its uint64 LE proto size,
// matching upstream backup.go writeKVList.
func writeKVListChunk(w *s2.Writer, list *badgerpb.KVList) error {
	buf, err := proto.Marshal(list)
	if err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(len(buf))); err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

func readMasterManifest(dir string) (*MasterManifest, error) {
	path := filepath.Join(dir, "manifest.json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &MasterManifest{Manifests: []*Manifest{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	var mm MasterManifest
	if err := json.Unmarshal(data, &mm); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}
	return &mm, nil
}

func writeManifestAtomic(path string, mm *MasterManifest) error {
	data, err := json.MarshalIndent(mm, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func newBackupID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// schemaPredicates returns the sorted list of predicate names currently in
// the schema state. Upstream stores this per-group in the manifest so
// restore can scope which predicates are repopulated.
func schemaPredicates() []string {
	state := schema.State()
	if state == nil {
		return nil
	}
	preds := state.Predicates()
	out := make([]string, 0, len(preds))
	for _, p := range preds {
		// Strip namespace prefix to keep manifests human-readable; restore
		// re-applies it via x.NamespaceAttr based on schema state.
		_, attr := splitNamespacedAttr(p)
		out = append(out, attr)
	}
	sort.Strings(out)
	return out
}

// splitNamespacedAttr strips the 8-byte namespace prefix from a predicate
// name. bonsai keeps predicates internally as `<8-byte-ns><attr>`. For the
// manifest we want the bare attr.
func splitNamespacedAttr(p string) (ns []byte, attr string) {
	if len(p) < 8 {
		return nil, p
	}
	return []byte(p[:8]), strings.TrimPrefix(p[8:], "")
}

// RestoreOptions configures RestoreFromManifest.
type RestoreOptions struct {
	// UntilTs caps the restore at a target ReadTs. Manifests in the chain
	// whose ReadTs > UntilTs are skipped. Use this for point-in-time
	// recovery: "give me the database as it looked at T".
	//
	// 0 (default) means restore the entire chain.
	UntilTs uint64
}

// RestoreFromManifest replays an upstream-compatible multi-file backup into
// the open DB. The directory must contain a manifest.json with a chain
// starting at BackupNum=1 (full) followed by zero or more incremental
// manifests sharing the same BackupID. Each backup file's KVList chunks are
// written through Badger's managed write batch at the versions they carry.
//
// Unlike RestoreFrom (which calls Badger Load and wipes existing data), this
// function applies on top of whatever is already in the DB. Call db.DropAll
// first if a clean slate is needed.
//
// For point-in-time recovery, use RestoreFromManifestWithOptions and pass
// RestoreOptions.UntilTs to stop at a target timestamp.
func (d *DB) RestoreFromManifest(ctx context.Context, dir string) error {
	return d.RestoreFromManifestWithOptions(ctx, dir, RestoreOptions{})
}

// RestoreFromManifestWithOptions is RestoreFromManifest with knobs. The
// most common knob is UntilTs for point-in-time recovery.
func (d *DB) RestoreFromManifestWithOptions(ctx context.Context, dir string, opts RestoreOptions) (err error) {
	defer d.auditDeferred("RestoreFromManifest", ctx,
		map[string]any{"dir": dir, "until_ts": opts.UntilTs}, &err)()
	master, err := readMasterManifest(dir)
	if err != nil {
		return err
	}
	if len(master.Manifests) == 0 {
		return fmt.Errorf("RestoreFromManifest: %s/manifest.json has no entries", dir)
	}

	// Validate the chain: first manifest must be full, all share BackupID,
	// BackupNum is monotonic from 1.
	if master.Manifests[0].Type != string(BackupFull) || master.Manifests[0].BackupNum != 1 {
		return fmt.Errorf("RestoreFromManifest: first manifest is not a full backup")
	}
	bid := master.Manifests[0].BackupID
	for i, m := range master.Manifests {
		if m.BackupID != bid {
			return fmt.Errorf("RestoreFromManifest: manifest[%d] BackupID %q does not match chain %q",
				i, m.BackupID, bid)
		}
		if m.BackupNum != uint64(i+1) {
			return fmt.Errorf("RestoreFromManifest: manifest[%d] BackupNum %d, expected %d",
				i, m.BackupNum, i+1)
		}
	}

	// PIT bound. The full backup (BackupNum=1) is always applied — it's
	// the floor, even if its ReadTs is past UntilTs the alternative is "no
	// data at all", which isn't a useful semantics. Incrementals past the
	// bound are skipped.
	if opts.UntilTs > 0 {
		full := master.Manifests[0]
		if full.ReadTs > opts.UntilTs {
			return fmt.Errorf("RestoreFromManifest: UntilTs %d is older than the full backup's ReadTs %d",
				opts.UntilTs, full.ReadTs)
		}
	}

	var maxVer uint64
	var applied int
	for _, m := range master.Manifests {
		if opts.UntilTs > 0 && m.ReadTs > opts.UntilTs {
			break
		}
		v, err := d.applyBackupFile(ctx, dir, m)
		if err != nil {
			return fmt.Errorf("apply %s: %w", m.Path, err)
		}
		applied++
		if v > maxVer {
			maxVer = v
		}
	}
	_ = applied // could surface this on the audit entry; kept local for now

	// Advance the timestamp counter past the highest restored version.
	target := maxVer + 1
	if cur := worker.CurrentTs() + 1; cur > target {
		target = cur
	}
	d.tsCount.Store(target)
	worker.SeedLocalTs(target)
	posting.ResetCache()
	posting.Oracle().ProcessDelta(&pb.OracleDelta{MaxAssigned: d.tsCount.Load()})
	return schema.LoadFromDb(ctx)
}

// applyBackupFile reads one r<ReadTs>-g1.backup file, snappy-decompresses
// it, and writes each KVList entry into Badger via a managed write batch.
// Returns the highest KV version encountered, used by the caller to advance
// the timestamp counter.
func (d *DB) applyBackupFile(_ context.Context, root string, m *Manifest) (uint64, error) {
	path := filepath.Join(root, m.Path, fmt.Sprintf("r%d-g1.backup", m.ReadTs))
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	// Compression field on the per-backup manifest may be empty (older
	// upstream backups), "snappy", or "gzip". bonsai only produces snappy.
	var r io.Reader = f
	switch m.Compression {
	case "", "snappy":
		r = s2.NewReader(f)
	default:
		return 0, fmt.Errorf("unsupported compression %q", m.Compression)
	}

	wb := d.pstore.NewManagedWriteBatch()
	defer wb.Cancel()

	var (
		szBuf  [8]byte
		maxVer uint64
	)
	for {
		if _, err := io.ReadFull(r, szBuf[:]); err == io.EOF {
			break
		} else if err != nil {
			return 0, fmt.Errorf("read size: %w", err)
		}
		size := binary.LittleEndian.Uint64(szBuf[:])
		if size == 0 {
			continue
		}
		buf := make([]byte, size)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, fmt.Errorf("read chunk: %w", err)
		}
		var list badgerpb.KVList
		if err := proto.Unmarshal(buf, &list); err != nil {
			return 0, fmt.Errorf("unmarshal: %w", err)
		}
		for _, kv := range list.Kv {
			if len(kv.Key) == 0 {
				continue
			}
			if kv.Version > maxVer {
				maxVer = kv.Version
			}
			entry := &badger.Entry{
				Key:      kv.Key,
				Value:    kv.Value,
				UserMeta: byteOrZero(kv.UserMeta),
			}
			if kv.ExpiresAt > 0 {
				entry.ExpiresAt = kv.ExpiresAt
			}
			if err := wb.SetEntryAt(entry, kv.Version); err != nil {
				return 0, fmt.Errorf("write entry: %w", err)
			}
		}
	}
	if err := wb.Flush(); err != nil {
		return 0, fmt.Errorf("flush: %w", err)
	}
	return maxVer, nil
}

func byteOrZero(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0]
}
