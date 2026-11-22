# Resumable crawling（断点续抓）

This document describes the resumable-crawl capability, how to use it, how it
is implemented, its durability/performance trade-offs, and its compatibility
boundaries. The capability is **opt-in**: without `-resume`, hakrawler behaves
exactly as it always did (same output, same exit codes, no files created).

## 1. Usage

```sh
# First run: enable resume and pick a state directory
cat urls.txt | hakrawler -resume ./crawl-state

# Machine reboot / Ctrl-C / kill: run the same command again
cat urls.txt | hakrawler -resume ./crawl-state
```

Already-fetched addresses are not fetched again; the crawl continues from the
first unfinished address. Previously failed addresses are retried and their
last failure type is kept.

### Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-resume <dir>` | _(empty = off)_ | Enable resumable crawling and keep all progress in this directory. |
| `-resume-sync <mode>` | `group` | Durability mode. `group` batches disk flushes (fast); `strict` flushes every record (zero record loss on kill, slower). |
| `-resume-sync-window <dur>` | `50ms` | In `group` mode, the maximum amount of progress that may sit in the OS page cache before being forced to disk. Lower = safer, higher = fewer sync points. |
| `-resume-snapshot <path>` | `<dir>/queue-snapshot.json` | Where the exportable queue/progress snapshot is written. |
| `-resume-snapshot-interval <dur>` | `5s` | How often the snapshot is refreshed while crawling. `0` disables periodic snapshots (a final snapshot is always written). |

All original flags (`-d`, `-t`, `-u`, `-json`, `-s`, `-w`, `-i`, `-subs`,
`-proxy`, `-timeout`, `-dr`, `-insecure`, `-size`, `-h`) keep working and have
the same meaning.

### State directory contents

```
crawl-state/
├── hakrawler.journal      # append-only, checksummed progress log
├── queue-snapshot.json    # exportable, human-readable progress snapshot
└── .lock                  # advisory lock preventing two concurrent runs
```

The snapshot answers "how far has it got and where is it stuck?":

```json
{
  "generated_at": "2026-11-22T00:00:00Z",
  "seeds": ["http://127.0.0.1:18099/"],
  "counts": { "completed": 5, "failed": 1 },
  "queue": [],
  "failed": [
    { "url": "http://127.0.0.1:18099/500", "fail_type": "http_status",
      "fail_detail": "HTTP 500", "attempts": 2 }
  ],
  "outputs_recorded": 8
}
```

`queue` lists addresses still to fetch (with depth and owning seed); `failed`
lists addresses whose last attempt failed and that will be retried.

## 2. URL identity

An address's identity is its **normalized** URL:

- the fragment (`#...`) is dropped — it never reaches the server;
- query parameters are sorted by `(key, value)`, so `?a=1&b=2` and `?b=2&a=1`
  are the same identity;
- the host is lower-cased.

Therefore `…/page?z=9&a=1#frag` and `…/page?a=1&z=9#other` are fetched **once**.
This normalization is applied at every journal entry point, so it is the single
identity authority for the whole run.

## 3. Durability model and the two sync modes

The journal is an append-only JSONL log. Every line carries a CRC32 checksum
of its payload. A torn final line (process killed mid-write) fails the
checksum on the next start and is reported as corruption rather than silently
trusted.

- **`strict`** — every appended record is `fsync`-ed before the call returns.
  A `SIGKILL` cannot lose a completed record. Cost: one synchronous point per
  record (~1.8 ms/record measured on WSL2 DrvFS), which is the dominant cost.
- **`group` (default)** — records reach the OS page cache immediately and a
  background flusher `fsync`s at most every `-resume-sync-window`. A hard kill
  can roll the journal back by at most one window. That is safe because the
  lost records describe work that is simply redone: a URL still marked
  `queued` is re-fetched, its links are de-duplicated by identity, and outputs
  are re-emitted through the same identity de-duplication. The final link set
  is unchanged.

In both modes the snapshot export is atomic (temp file + `fsync` + `rename`).

### Measured overhead (local test site, same input, `-t 16 -d 2 -u`)

| Pages | One-shot | Resume `group` | Resume `strict` |
|------:|---------:|---------------:|----------------:|
| 2,000 | ~1.5 s | ~1.5 s | ~12 s |
| 5,000 | ~3.3 s | ~3.35 s | ~24–33 s |

(Wall-clock median of three runs on this host. Absolute numbers are
machine/FS dependent; the conclusion is that `group` adds no material overhead
while `strict` trades speed for zero-loss durability.)

**New synchronous points / copies introduced**

- One-shot: none.
- Resume `group`: one `fsync` per sync window (~20 for a 1 s crawl) instead of
  one per record; no extra in-memory copies beyond the small JSON line.
- Resume `strict`: one `fsync` per state/output record (queued, completed,
  failed, each printed result) — the source of its slowdown.
- A journal **compaction** runs at start once the file exceeds 16 MiB, folding
  history to one record per URL/output via an atomic temp-file rename; this is
  a bounded one-time rewrite, not a per-request copy.

## 4. Failure handling and recovery messages

| Situation | Behaviour |
|-----------|-----------|
| State directory not writable | Refused at startup before any crawl, with the offending path. |
| Journal checksum mismatch / torn line | Exit non-zero: `journal corrupt: <file> line N …` plus a `recovery:` instruction (move the file to `.corrupt` and rerun for a fresh crawl, or restore from backup). |
| Seed list replaced/shortened | Exit non-zero: `seed list changed … refusing to resume`, because re-attributing queued URLs to a different seed would mis-attribute scope. Use the same seeds or a new state directory. |
| New seed added | Merged into the queue; already-completed work is reused (`N new seed(s) merged into the queue` on stderr). |
| Two processes, same directory | The second fails immediately: the directory is already in use (advisory `flock`). |
| Re-interrupted while resuming | All records written so far are durable (strict) or durable within one window (group); no progress from the current run is lost beyond that bound. |
| A URL failed last run | Retried; its previous `fail_type` (`http_status` / `transport` / `malformed`) and detail remain traceable in the snapshot. |

## 5. Concurrency and extreme-input determinism

- All journal mutation is serialized by one mutex; the on-disk log is
  append-only. Concurrent workers discovering the same normalized URL race on
  `MarkQueued`; exactly one wins and the rest skip it, so an address is
  fetched once regardless of thread count.
- **Peer disconnect / connection reset / timeout**: colly invokes `OnError`;
  the URL is classified `transport` and marked `failed`, stays pending, and is
  retried next run. No panic, no hang.
- **Malformed upstream body**: classified `malformed`; the same retry path
  applies. Classification is a pure function of the error/status, so the same
  input always yields the same category.
- **HTTP 5xx**: recorded as `http_status` with the status code; a permanently
  failing URL ends `failed` (never silently dropped) and is retried each run.
- A kill between a child being journaled and its parent being completed leaves
  the parent `queued`; on resume the parent is re-fetched but every child is
  already de-duplicated — no link lost, no duplicate output.

## 6. Why not reuse colly's built-in storage/queue?

colly ships `storage.Storage` (default `InMemoryStorage`) and a `queue` package.
They are not sufficient here:

1. colly's visited store keeps only an **FNV-64 hash of the raw URL string**.
   It cannot reconstruct the pending queue, cannot report which address is
   stuck, and hashes the fragment/parameter spelling verbatim — so the two
   spellings that this feature must treat as one identity are seen as two
   different visits (this is why the original one-shot path emits duplicate
   lines for fragment variants).
2. The bundled queue storage is in-memory only; durable backends require
   external systems (SQL/Redis). hakrawler does not currently use
   `colly.Queue` at all — its frontier is the in-memory async `Visit` graph —
   so adopting it would change single-shot ordering and depth behaviour.
3. Neither mechanism offers atomic crash-safe writes, seed-list
   reconciliation, failure-type history, or output replay.

**Coexistence:** the single-shot path keeps colly's default `InMemoryStorage`
completely untouched. The resume path runs the same collector construction but
layers the journal beside it as the cross-process identity authority; colly's
per-process in-memory de-duplication still works inside each run.

## 7. Compatibility boundary (old data)

- This feature only affects runs that pass `-resume`. Old runs and old output
  files are untouched; there is no on-disk state to migrate.
- A state directory written by one hakrawler build is read through the
  versioned journal format (`"v":1`). If a future format changes
  incompatibly, the version field is the gate; today there is only v1.
- The journal is an implementation detail, not a stable public API: inspect it
  for debugging, but drive automation from `queue-snapshot.json`. To discard
  progress and start clean, delete or move the state directory.

## 8. Known limitations

- Cookies/session state are not persisted (colly's cookie jar stays in
  memory); only URL progress, outputs and failure history survive a restart.
- `group` mode can redo up to one sync window of work after a hard kill (by
  design). Use `-resume-sync strict` if zero record loss is required.
- Journal growth is bounded by the 16 MiB compaction threshold, but a very
  large one-shot discovery (millions of result lines) still produces a
  proportionally large journal because every printed line is recorded.
- `-timeout` retains its per-seed semantics; on timeout the run ends and the
  remaining URLs are retried on the next invocation rather than within the
  same process.
