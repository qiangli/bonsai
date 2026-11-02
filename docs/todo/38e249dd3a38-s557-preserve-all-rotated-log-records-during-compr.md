---
id: 38e249dd3a38
kind: task
title: S557 preserve all rotated log records during compression shutdown
seq: 7
status: assigned
created: 2026-11-02T03:26:49.850274Z
assignee: s557-public-tooling
sprint: 557
sprint_id: f5ccf563-f5d1-5b94-8be4-9388e51ef86d
sprint_title: 'Bashy v1.0.0 preparation: split private/public development and retire qiangli/dhnt'
---

Hosted native-repairs41381899440 exact umbrella8325b1b Linux TestLogWriterWithCompression reports8977 lines versus10000 after Close; macOS/Windows pass. Diagnose bounded reproduction and writer Close/flush/compression lifecycle, preserve count/assertions, no sleeps. Repair only with source or reproduction evidence; isolated worktree, focused race tests, root reviews and publishes.

Implementation checkpoint:
- Rotation used millisecond-only names without collision checks. A 1024-byte
  record and the >=1MiB rotation condition put 1023 records in each backup;
  the hosted deficit is exactly one backup. Reusing its pathname can replace
  an earlier backup on Unix and race compression of the same pathname.
- Rotation now advances a logical millisecond timestamp under the writer mutex
  and checks both raw and compressed names already on disk. The existing
  timestamp filename format remains unchanged. Concurrent independent writers
  targeting the same active file are not introduced or supported by this fix.
- Authored deterministic guards cover existing raw/compressed backups, clock
  rollback, and two actual rotations preserving all three distinct records.
- Baseline Darwin compression test passed 20 repetitions (2.250s), so the
  hosted Linux failure has not been reproduced locally. Source evidence is
  the basis for this repair; no platform pass is inferred from it.
- Focused race selection (ordinary writer, compression, encryption, collision,
  actual rotation preservation), ten repetitions each, passed in 7.433s.
  The existing 10000-record assertions and worker joins are unchanged.
- Full hosted verification and manager acceptance remain pending.
