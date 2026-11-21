# Boundary mode: design, concurrency and compatibility

This document accompanies the `-boundary` feature and records the decisions
that the README's usage section does not have room for.

## 1. Where the boundary checks run

The legacy pipeline extracts a link inside a colly `OnHTML` callback and calls
`e.Request.Visit(link)`. Colly then spawns the request goroutine first and
only afterwards runs `requestCheck()` — depth, domain filter and the
raw-URL revisit store all execute *after* scheduling. Two consequences:

1. a link over the depth limit still allocates a goroutine and passes through
   the HTTP backend's semaphore before being rejected;
2. the revisit store hashes the literal URL string, so fragment/parameter
   order/host-case variants are treated as different resources.

Boundary mode does not call `Visit` from the extraction callbacks at all.
Callbacks only record discoveries; the BFS driver then runs every link
through `Guard.Classify(link, parentDepth+1)` and only the admitted hrefs are
handed to `Visit`. The check is therefore structurally *before* enqueue, not
merely before the HTTP call — test `TestDepthCheckBeforeEnqueue` asserts this
by recording every path the server actually receives and failing if a
depth-refused page is ever requested.

## 2. Why the colly-native controls are not reused

Colly already offers controls that look similar, but with different
semantics; they cannot express the required accounting.

| Need | Colly-native option | Why it is not reused | How the two coexist |
|------|--------------------|----------------------|---------------------|
| Depth limit | `MaxDepth` | Checked inside `scrape`, after the goroutine is spawned; returns `ErrMaxDepth` to a caller that ignores it, so the skip is invisible. | Boundary mode leaves `MaxDepth` at 0 (unlimited) and enforces depth in the guard with a counter. Legacy mode still uses `MaxDepth` unchanged. |
| Domain scope | `AllowedDomains` / `URLFilters` | Host-only or regex matching; no eTLD+1 "same registrable domain" mode, no whitelist-with-subdomains, no per-link rejection reason. | The boundary collector sets neither; the guard owns scope. Legacy mode keeps `AllowedDomains` and the `-subs` regex. |
| Dedup | in-memory revisit store (FNV hash of raw URL) | Identity is the literal string; fragments and query permutations are distinct. No duplicate count is exposed. | Boundary mode sets `AllowURLRevisit()` and dedups on `NormalizeURL` output via its own `sync.Map`. Legacy mode keeps colly's store. |
| Static/junk filtering | `DisallowedURLFilters` | Silent rejection, no rule names, no hit counts; rules would also apply inside colly rather than before enqueue. | The guard's named `ExclusionRule` list does the counting. `DisallowedURLFilters` stays unused in both modes. |
| Output uniqueness | `-u` + process-global `sync.Map` of printed strings | Dedup key is the un-normalised output line, so it cannot collapse writings of one resource. | In boundary mode `-u` dedups on the same canonical key as crawl dedup. Legacy `-u` is untouched. |

Both code paths remain in the binary behind a single `-boundary` switch, so
the old and new semantics coexist without a fork in the data model.

## 3. Concurrency model and extreme-case behaviour

### Deterministic ordering

Colly fetches one goroutine per request, limited by the `-t` semaphore in
the HTTP backend. Boundary mode keeps that parallelism but processes the
graph breadth-first, one depth level at a time:

- all pages of level N are scheduled, then a per-level `WaitGroup` barrier
  (`OnScraped`/`OnError` each release one slot, reserved with `Add` *before*
  `Visit`) waits for every terminal callback;
- discoveries are appended to a slice under a mutex and assigned a global
  in-page sequence number;
- after the barrier, the slice is sorted by `(page URL, sequence, source,
  raw value)` and flushed before level N+1 is scheduled.

Goroutine scheduling therefore affects only wall time, never output order.
`TestDeterministicOutputOrder` runs the same crawl five times and asserts
byte-identical output; the benchmark runs in `artifacts/` showed the same.

### Peer disconnect / malformed upstream

Every failure colly can produce lands in one `OnError` callback and releases
the barrier slot, so a failing request can never deadlock a level:

- transport failure (connection reset, `unexpected EOF`, truncated body,
  deadline) — `Response` is nil or status 0 → `fetch_errors`;
- HTTP 4xx/5xx — logged, not counted as a pipeline error;
- parse failure after headers — `parse_errors`.

The page is skipped; its siblings already scheduled continue, and the next
level is built only from links that survived the guard. No state is shared
between requests except the mutex-guarded discovery slice, the atomic
counters and the `sync.Map` dedup set, all of which are concurrency-safe.
`TestMalformedUpstreamDoesNotKillCrawl` drives a hijacked connection that
closes mid-response alongside a healthy page and asserts the healthy crawl
still completes.

### Timeout

`-timeout` becomes a `context.WithTimeout` on the collector. On expiry the
in-flight requests are cancelled (they surface as transport errors and are
counted), the current level drains, the seed is marked in
`timed_out_seeds`, and processing moves to the next seed. No goroutine
outlives `crawlSeed` — the context's `cancel` runs on every return path.

### Sync points and copies (performance)

Added relative to the legacy path:

- one mutex critical section per extracted link (an append of a small
  struct, microseconds);
- one atomic add per counter update;
- one `sync.Map` lookup-or-store per candidate;
- one normalisation per link (URL parse + query sort, allocation-light);
- one level-wide sort and one buffered flush per depth level.

There is **no new blocking network synchronisation**: the level barrier waits
on the same request completion the legacy code's `c.Wait()` already waits on,
just implemented with a reusable WaitGroup because colly's internal one
cannot be waited on twice. The extra copies are the per-level discovery
slice (one struct per link) and the sorted output buffer for that level;
nothing accumulates across levels, so memory stays bounded by the widest
level rather than the whole site, and results still stream to stdout level by
level.

## 4. Measured before/after comparison

Local deterministic fixture (in-memory HTTP server, same binary host,
`-t 16 -d 3`; wall time from `time`, median of 3–5 runs):

| Site | Mode | Wall time | Peak RSS | Output lines |
|------|------|-----------|----------|--------------|
| 200 pages, ~12.4k extracted links | legacy | 0.07 s | ~48 MB | 19,522 |
| 200 pages, same input | boundary | 0.08 s | ~31 MB | 12,370 |
| 2,000 pages, same link graph | legacy | 0.09 s | ~53 MB | 19,522 |
| 2,000 pages, same input | boundary | 0.10 s | ~31 MB | 12,370 |

The line-count difference is the feature working, not data loss: legacy
re-prints every duplicate writing and every excluded link it schedules, while
boundary mode prints each extracted link once in deterministic order and
accounts for the rest in the stats. The stdout *record shape*
(`Source`/`URL`/`Where`, plain or JSON) is unchanged — verified by a
field-level diff in the acceptance run. Wall time is within run-to-run
noise on this workload; the guard work is CPU-bound and negligible next to
even loopback I/O. On a high-latency crawl the per-level barrier can add at
most one level's worth of scheduling latency versus fully asynchronous
dispatch, traded for deterministic output and pre-enqueue enforcement.

## 5. Compatibility boundaries and old data

- **Old command lines**: every existing flag keeps its behaviour. Without
  `-boundary`, the legacy collector code runs unchanged and no new file or
  output field is produced. The default scope (`host`) matches the legacy
  default of restricting to the seed's exact host.
- **Old output consumers**: stdout is the same format with the same exit
  codes for existing outcomes. New exit code `2` is only returned in
  boundary mode when all seeds are rejected. Stats go to stderr or a
  `-stats` file, never stdout.
- **Old collected data**: the feature changes what is *fetched and
  reported* on new runs; it does not rewrite or understand previously saved
  crawl output. There is no on-disk crawl state to migrate. If you want old
  runs deduplicated under the new identity rules, re-run them through
  boundary mode (or pipe an old URL list through the normaliser by using it
  as stdin with `-boundary -d 1`); the old files themselves need no
  conversion and remain valid inputs.
- **Go version**: the module directive moves from 1.16 to 1.19 because the
  counters use `sync/atomic.Int64`. No other toolchain requirement changes.

## 6. Acceptance cross-reference

| Requirement | Where satisfied |
|-------------|-----------------|
| Four cases: depth / scope / rule / normalisation | `TestIntegrationDepthExceeded`, `TestIntegrationScopeExclusion`, `TestIntegrationRuleExclusion`, `TestIntegrationCanonicalDedup` |
| Depth enforced before enqueue | `TestDepthCheckBeforeEnqueue` (server-side request recorder) |
| Buckets sum to candidate total | `TestStatsInvariantBucketsSumToCandidates`, `TestIntegrationInvariant`, plus the printed invariant line |
| Single normalisation definition | `normalize.go`, used by seed validation, guard, dedup and `-u` |
| Output format / exit codes unchanged | legacy path untouched; record-shape diff in §4 |
| Stable order under concurrency | `TestDeterministicOutputOrder` |
| Large-site streaming | per-level flush; bounded by widest level (§3) |
| Parse failure skipped and counted | `TestMalformedUpstreamDoesNotKillCrawl` |
| Out-of-scope seed refused at startup | `TestSeedRejectionExitPath`, `TestSeedRejectedAtStartup` |
| No half-written stats file | `writeAtomicJSON` (temp + fsync + rename) |
