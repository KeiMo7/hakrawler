package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

// Journal file name inside the state directory.
const journalFileName = "hakrawler.journal"

// defaultSyncWindow bounds how long group-commit mode may leave records only
// in the OS page cache before forcing them to disk. At this size a 2k-page
// crawl costs roughly one fsync per 50 ms (~30 synchronous points instead of
// ~6000), which measured equal to the one-shot runtime, while a crash still
// redoes at most a 50 ms slice of work.
const defaultSyncWindow = 50 * time.Millisecond

// URL lifecycle states stored in the journal.
const (
	statusQueued    = "queued"    // discovered, not fetched yet
	statusCompleted = "completed" // fetched without a transport/parse error
	statusFailed    = "failed"    // last attempt failed; retried on the next run
)

// Failure categories kept on failed entries so the previous failure type stays
// traceable across a resume.
const (
	failHTTP      = "http_status" // server replied with a non-2xx status
	failTransport = "transport"   // connection reset, DNS, timeout, peer disconnect ...
	failMalformed = "malformed"   // upstream body could not be parsed
)

// journalEntry is one append-only event. Every line on disk is
//
//	{"v":1,"crc":<crc32 of the JSON of the inner fields>, ...fields}
//
// The checksum makes a half-written line (process killed mid-write, torn
// sector) distinguishable from a valid one: replay stops at the first bad
// line instead of silently accepting truncated state.
type journalEntry struct {
	V   int    `json:"v"`
	CRC uint32 `json:"crc"`

	Kind       string   `json:"kind"`            // seeds | state | output
	Seeds      []string `json:"seeds,omitempty"` // kind=seeds: normalized seed identities
	URL        string   `json:"url,omitempty"`   // kind=state: normalized URL identity
	Status     string   `json:"status,omitempty"`
	Depth      int      `json:"depth,omitempty"`
	Seed       string   `json:"seed,omitempty"`   // seed this URL belongs to
	Output     string   `json:"output,omitempty"` // kind=output: exact stdout line
	Seq        int64    `json:"seq,omitempty"`    // discovery order, for deterministic replay
	FailType   string   `json:"fail_type,omitempty"`
	FailDetail string   `json:"fail_detail,omitempty"`
	Attempts   int      `json:"attempts,omitempty"`
	TS         string   `json:"ts,omitempty"`
}

// urlState is the in-memory projection of one URL.
type urlState struct {
	Status     string
	Depth      int
	Seed       string
	FailType   string
	FailDetail string
	Attempts   int
}

// Journal is the durable progress store. Writes are append-only and
// serialised by mu. Durability is provided by two modes:
//
//   - syncModeStrict: every append is fsynced before the call returns. A
//     SIGKILL can never lose a completed journal record, but each fsync is a
//     synchronous point (measured ~1.8 ms/record on WSL2 DrvFS).
//   - syncModeGroup (default): appends reach the OS page cache immediately
//     and a background flusher fsyncs at most every syncWindow. A SIGKILL can
//     therefore roll the journal back by at most one window; the records lost
//     are exactly the ones whose work is redone on resume (a URL still marked
//     queued is refetched and its links are deduped, so neither the final set
//     nor output correctness changes).
//
// In both modes a kill leaves either whole lines or no new line (writes are
// small and the checksum rejects any torn line on replay), never a mixed or
// half-parseable state that the checksum cannot detect.
type Journal struct {
	mu         sync.Mutex
	path       string
	f          *os.File
	lockF      *os.File
	seeds      []string
	urls       map[string]*urlState
	outputs    []outputRecord // exact stdout lines in discovery order
	seq        int64
	syncMode   string
	syncWindow time.Duration

	// group-commit plumbing
	wakeCh chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}
	closed bool
}

// Sync modes selectable via -resume-sync.
const (
	syncModeGroup  = "group"
	syncModeStrict = "strict"
)

type outputRecord struct {
	Seq    int64  `json:"seq"`
	URL    string `json:"url"`    // normalized identity of the *result* URL
	Output string `json:"output"` // exact line previously printed
}

var (
	errCorruptJournal = errors.New("journal corrupt")
	errSeedsChanged   = errors.New("seed list changed")
	errNotAbsolute    = errors.New("input must be a valid absolute URL")
)

// assertStateDirWritable fails the run *before* any crawling starts when the
// state directory cannot be created or written to.
func assertStateDirWritable(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("state directory %q not writable: %w", dir, err)
	}
	probe := filepath.Join(dir, ".write-probe")
	f, err := os.OpenFile(probe, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("state directory %q not writable: %w", dir, err)
	}
	if _, err := f.WriteString("ok"); err != nil {
		f.Close()
		return fmt.Errorf("state directory %q not writable: %w", dir, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("state directory %q not writable: %w", dir, err)
	}
	f.Close()
	os.Remove(probe)
	return nil
}

// openJournal opens (or creates) the journal under dir and takes an advisory
// exclusive lock so two resume runs cannot interleave appends into one file.
// syncMode is syncModeGroup or syncModeStrict; window bounds the batching
// delay in group mode.
func openJournal(dir, syncMode string, window time.Duration) (*Journal, error) {
	if err := assertStateDirWritable(dir); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, journalFileName)

	lockPath := filepath.Join(dir, ".lock")
	lockF, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lockF.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lockF.Close()
		return nil, fmt.Errorf("state directory %q is already in use by another hakrawler process", dir)
	}

	if syncMode != syncModeGroup && syncMode != syncModeStrict {
		lockF.Close()
		return nil, fmt.Errorf("unknown sync mode %q (want %q or %q)", syncMode, syncModeGroup, syncModeStrict)
	}
	if window <= 0 {
		window = defaultSyncWindow
	}

	j := &Journal{
		path:       path,
		lockF:      lockF,
		urls:       make(map[string]*urlState),
		syncMode:   syncMode,
		syncWindow: window,
	}

	if err := j.replay(); err != nil {
		// keep the lock file open; return a structured corruption error
		return nil, err
	}

	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	j.f, err = os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}

	if syncMode == syncModeGroup {
		j.wakeCh = make(chan struct{}, 1)
		j.stopCh = make(chan struct{})
		j.doneCh = make(chan struct{})
		go j.flusher()
	}
	return j, nil
}

// flusher fsyncs pending appends at most every syncWindow, coalescing bursts
// of records into one synchronous point. It exits on Stop and performs a
// final fsync so a clean shutdown loses nothing.
func (j *Journal) flusher() {
	defer close(j.doneCh)
	t := time.NewTimer(j.syncWindow)
	defer t.Stop()
	for {
		select {
		case <-j.stopCh:
			j.flushNow()
			return
		case <-j.wakeCh:
			// an append arrived; wait out the window to coalesce, then flush
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			t.Reset(j.syncWindow)
			select {
			case <-t.C:
				j.flushNow()
			case <-j.stopCh:
				j.flushNow()
				return
			}
		}
	}
}

// flushNow performs one fsync under the journal lock.
func (j *Journal) flushNow() {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f != nil {
		_ = j.f.Sync()
	}
}

// replay reads every journal line, validating each checksum. A bad line that
// is not the last line, or a bad final line that is not a clean prefix (torn
// append), is fatal.
func (j *Journal) replay() error {
	f, err := os.Open(j.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := json.NewDecoder(f)
	lineNo := 0
	for {
		lineNo++
		var raw json.RawMessage
		if err := r.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// Decoder error: typically a torn final line (partial JSON).
			return j.corruptError(lineNo, err.Error())
		}
		var e journalEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return j.corruptError(lineNo, err.Error())
		}
		want := e.CRC
		e.CRC = 0
		canon, err := json.Marshal(e)
		if err != nil {
			return j.corruptError(lineNo, err.Error())
		}
		if crc32.ChecksumIEEE(canon) != want {
			return j.corruptError(lineNo, "checksum mismatch")
		}
		if err := j.apply(e); err != nil {
			return j.corruptError(lineNo, err.Error())
		}
	}
}

func (j *Journal) corruptError(line int, detail string) error {
	return fmt.Errorf("%w: %s line %d: %s\nrecovery: the state file is not trusted. "+
		"Inspect %s, then either move it aside (e.g. mv %s %s.corrupt) and rerun to start a fresh crawl, "+
		"or restore it from backup and rerun to resume from the last intact record",
		errCorruptJournal, j.path, line, detail, j.path, j.path, j.path)
}

func (j *Journal) apply(e journalEntry) error {
	switch e.Kind {
	case "seeds":
		j.seeds = append([]string(nil), e.Seeds...)
	case "state":
		st := &urlState{
			Status:     e.Status,
			Depth:      e.Depth,
			Seed:       e.Seed,
			FailType:   e.FailType,
			FailDetail: e.FailDetail,
			Attempts:   e.Attempts,
		}
		j.urls[e.URL] = st
	case "output":
		j.outputs = append(j.outputs, outputRecord{Seq: e.Seq, URL: e.URL, Output: e.Output})
		if e.Seq > j.seq {
			j.seq = e.Seq
		}
	default:
		return fmt.Errorf("unknown entry kind %q", e.Kind)
	}
	return nil
}

// appendEntry computes the checksum over the entry without its crc field and
// performs one write. In strict mode it also fsyncs before returning; in group
// mode it signals the background flusher instead. Caller must hold j.mu.
func (j *Journal) appendEntry(e journalEntry) error {
	e.V = 1
	e.TS = time.Now().UTC().Format(time.RFC3339Nano)
	e.CRC = 0
	canon, err := json.Marshal(e)
	if err != nil {
		return err
	}
	e.CRC = crc32.ChecksumIEEE(canon)
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := j.f.Write(line); err != nil {
		return err
	}
	if j.syncMode == syncModeStrict {
		if err := j.f.Sync(); err != nil {
			return err
		}
	} else if j.wakeCh != nil {
		select {
		case j.wakeCh <- struct{}{}:
		default: // a flush is already pending
		}
	}
	return nil
}

// InitSeeds records the seed set of a fresh run or verifies an existing
// journal against the current stdin seed list.
//
// Reconciliation rule:
//   - new run (no journal): seeds are normalised and stored.
//   - resume: every previous seed must still be present. Removals/replacements
//     are rejected (errSeedsChanged). New seeds are merged in and enqueued.
func (j *Journal) InitSeeds(rawSeeds []string) (added []string, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	normalized := make([]string, 0, len(rawSeeds))
	seen := map[string]bool{}
	for _, r := range rawSeeds {
		n, nerr := normalizeURL(r)
		if nerr != nil {
			return nil, fmt.Errorf("invalid seed %q: %w", r, nerr)
		}
		if !seen[n] {
			seen[n] = true
			normalized = append(normalized, n)
		}
	}

	if len(j.seeds) == 0 {
		if err := j.appendEntry(journalEntry{Kind: "seeds", Seeds: normalized}); err != nil {
			return nil, err
		}
		j.seeds = normalized
		return normalized, nil
	}

	prev := map[string]bool{}
	for _, s := range j.seeds {
		prev[s] = true
	}
	for _, s := range normalized {
		if !prev[s] {
			added = append(added, s)
		}
	}
	var removed []string
	cur := map[string]bool{}
	for _, s := range normalized {
		cur[s] = true
	}
	for _, s := range j.seeds {
		if !cur[s] {
			removed = append(removed, s)
		}
	}
	if len(removed) > 0 {
		return nil, fmt.Errorf("%w: seed list no longer contains %v; refusing to resume (a replaced/removed seed would misattribute queued URLs). "+
			"Recovery: use the same seed list, or start a new run against a new state directory",
			errSeedsChanged, removed)
	}
	if len(added) > 0 {
		merged := append(append([]string(nil), j.seeds...), added...)
		if err := j.appendEntry(journalEntry{Kind: "seeds", Seeds: merged}); err != nil {
			return nil, err
		}
		j.seeds = merged
	}
	return added, nil
}

// MarkQueued records a newly discovered URL. The URL is normalised here as
// the single identity chokepoint; it returns false when the identity is
// already known so the caller skips re-enqueueing it.
func (j *Journal) MarkQueued(rawURL string, depth int, seed string) (bool, error) {
	normURL, err := normalizeURL(rawURL)
	if err != nil {
		return false, err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, ok := j.urls[normURL]; ok {
		return false, nil
	}
	if err := j.appendEntry(journalEntry{
		Kind: "state", URL: normURL, Status: statusQueued, Depth: depth, Seed: seed,
	}); err != nil {
		return false, err
	}
	j.urls[normURL] = &urlState{Status: statusQueued, Depth: depth, Seed: seed}
	return true, nil
}

// MarkCompleted records a finished, non-failed fetch.
func (j *Journal) MarkCompleted(rawURL string) error {
	normURL, err := normalizeURL(rawURL)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	st := j.urls[normURL]
	if st == nil {
		return fmt.Errorf("internal: completing unknown URL %s", normURL)
	}
	if err := j.appendEntry(journalEntry{
		Kind: "state", URL: normURL, Status: statusCompleted,
		Depth: st.Depth, Seed: st.Seed, Attempts: st.Attempts + 1,
	}); err != nil {
		return err
	}
	st.Status = statusCompleted
	st.Attempts++
	st.FailType = ""
	st.FailDetail = ""
	return nil
}

// MarkFailed records a failed fetch with a traceable category, keeping the
// URL eligible for a retry on the next run.
func (j *Journal) MarkFailed(rawURL, failType, detail string) error {
	normURL, err := normalizeURL(rawURL)
	if err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	st := j.urls[normURL]
	if st == nil {
		return fmt.Errorf("internal: failing unknown URL %s", normURL)
	}
	if err := j.appendEntry(journalEntry{
		Kind: "state", URL: normURL, Status: statusFailed,
		Depth: st.Depth, Seed: st.Seed, FailType: failType, FailDetail: detail,
		Attempts: st.Attempts + 1,
	}); err != nil {
		return err
	}
	st.Status = statusFailed
	st.FailType = failType
	st.FailDetail = detail
	st.Attempts++
	return nil
}

// RecordOutput stores an exact stdout line in discovery order. Every printed
// line is stored, so a non-`-u` run replays the same duplicated lines that a
// one-shot run would have produced; the -u de-duplication is applied on the
// output channel in both paths and is intentionally not baked into the
// journal. Replays across runs are naturally deduplicated because a
// completed URL is never refetched, so it never re-emits.
func (j *Journal) RecordOutput(resultURL, line string) (int64, error) {
	norm, err := normalizeURL(resultURL)
	if err != nil {
		// unparseable result (e.g. javascript:, mailto:) keep a raw identity
		norm = resultURL
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.seq++
	if err := j.appendEntry(journalEntry{
		Kind: "output", URL: norm, Output: line, Seq: j.seq,
	}); err != nil {
		return 0, err
	}
	j.outputs = append(j.outputs, outputRecord{Seq: j.seq, URL: norm, Output: line})
	return j.seq, nil
}

// Snapshot is an exportable, consistent view of the progress.
type Snapshot struct {
	GeneratedAt     string         `json:"generated_at"`
	Seeds           []string       `json:"seeds"`
	Counts          map[string]int `json:"counts"`
	Queue           []QueueItem    `json:"queue"`
	Failed          []FailedItem   `json:"failed"`
	OutputsRecorded int            `json:"outputs_recorded"`
}

// QueueItem is one pending address with enough context to see where a crawl
// is stuck.
type QueueItem struct {
	URL   string `json:"url"`
	Depth int    `json:"depth"`
	Seed  string `json:"seed"`
}

// FailedItem carries the previous failure type of an address.
type FailedItem struct {
	URL        string `json:"url"`
	FailType   string `json:"fail_type"`
	FailDetail string `json:"fail_detail"`
	Attempts   int    `json:"attempts"`
}

func (j *Journal) snapshotLocked() Snapshot {
	snap := Snapshot{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Seeds:       append([]string(nil), j.seeds...),
		Counts:      map[string]int{},
		Queue:       []QueueItem{},
		Failed:      []FailedItem{},
	}
	for u, st := range j.urls {
		snap.Counts[st.Status]++
		switch st.Status {
		case statusQueued:
			snap.Queue = append(snap.Queue, QueueItem{URL: u, Depth: st.Depth, Seed: st.Seed})
		case statusFailed:
			snap.Failed = append(snap.Failed, FailedItem{
				URL: u, FailType: st.FailType, FailDetail: st.FailDetail, Attempts: st.Attempts,
			})
		}
	}
	sort.Slice(snap.Queue, func(a, b int) bool { return snap.Queue[a].URL < snap.Queue[b].URL })
	sort.Slice(snap.Failed, func(a, b int) bool { return snap.Failed[a].URL < snap.Failed[b].URL })
	snap.OutputsRecorded = len(j.outputs)
	return snap
}

// Snapshot returns a point-in-time copy safe to serialise outside the lock.
func (j *Journal) Snapshot() Snapshot {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.snapshotLocked()
}

// ExportSnapshot writes the queue/progress snapshot atomically
// (tmp file + fsync + rename) so an export can never be half-written either.
func (j *Journal) ExportSnapshot(path string) error {
	j.mu.Lock()
	snap := j.snapshotLocked()
	j.mu.Unlock()

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Pending returns the normalized URLs that still need a fetch: queued and
// previously failed (failed URLs are retried). Order is deterministic.
func (j *Journal) Pending() []QueueItem {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []QueueItem
	for u, st := range j.urls {
		if st.Status == statusQueued || st.Status == statusFailed {
			out = append(out, QueueItem{URL: u, Depth: st.Depth, Seed: st.Seed})
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].Seed != out[b].Seed {
			return out[a].Seed < out[b].Seed
		}
		if out[a].Depth != out[b].Depth {
			return out[a].Depth < out[b].Depth
		}
		return out[a].URL < out[b].URL
	})
	return out
}

// State and accessors used by the runner and tests.
func (j *Journal) StateOf(normURL string) (urlState, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	st, ok := j.urls[normURL]
	if !ok {
		return urlState{}, false
	}
	return *st, true
}

func (j *Journal) Seeds() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.seeds...)
}

// ReplayOutputs returns stored stdout lines in the order they were first
// discovered, so a resumed run reproduces the previous run's output.
func (j *Journal) ReplayOutputs() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	recs := append([]outputRecord(nil), j.outputs...)
	sort.Slice(recs, func(a, b int) bool { return recs[a].Seq < recs[b].Seq })
	lines := make([]string, len(recs))
	for i, r := range recs {
		lines[i] = r.Output
	}
	return lines
}

// compactThresholdBytes triggers a compaction when the append-only journal
// grows past it. 20k pages of small JSON lines stays comfortably under it on
// a first run; repeated runs that keep appending state transitions are folded
// back to one record per URL/output. It is a variable so tests can lower it.
var compactThresholdBytes int64 = 16 << 20 // 16 MiB

// Compact rewrites the journal as one record per current URL/output plus the
// current seed set, dropping superseded history. The rewrite is atomic
// (tmp + fsync + rename) and runs under the journal lock, so a kill at any
// point leaves either the old or the new complete file.
func (j *Journal) Compact() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	info, err := j.f.Stat()
	if err != nil {
		return err
	}
	if info.Size() < compactThresholdBytes {
		return nil
	}
	tmp, err := os.OpenFile(j.path+".compact", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	write := func(e journalEntry) error {
		e.V = 1
		e.TS = time.Now().UTC().Format(time.RFC3339Nano)
		e.CRC = 0
		canon, err := json.Marshal(e)
		if err != nil {
			return err
		}
		e.CRC = crc32.ChecksumIEEE(canon)
		line, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, err = tmp.Write(append(line, '\n'))
		return err
	}

	if err := write(journalEntry{Kind: "seeds", Seeds: append([]string(nil), j.seeds...)}); err != nil {
		tmp.Close()
		return err
	}
	// Deterministic on-disk order keeps compacted files diff-stable.
	urlKeys := make([]string, 0, len(j.urls))
	for k := range j.urls {
		urlKeys = append(urlKeys, k)
	}
	sort.Strings(urlKeys)
	for _, k := range urlKeys {
		st := j.urls[k]
		if err := write(journalEntry{
			Kind: "state", URL: k, Status: st.Status, Depth: st.Depth, Seed: st.Seed,
			FailType: st.FailType, FailDetail: st.FailDetail, Attempts: st.Attempts,
		}); err != nil {
			tmp.Close()
			return err
		}
	}
	recs := append([]outputRecord(nil), j.outputs...)
	sort.Slice(recs, func(a, b int) bool { return recs[a].Seq < recs[b].Seq })
	for _, r := range recs {
		if err := write(journalEntry{Kind: "output", URL: r.URL, Output: r.Output, Seq: r.Seq}); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), j.path); err != nil {
		return err
	}
	// Reopen the append handle against the replaced inode.
	if err := j.f.Close(); err != nil {
		return err
	}
	j.f, err = os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	return err
}

// Close stops the group-commit flusher (performing a final fsync), closes the
// journal, and releases the advisory lock.
func (j *Journal) Close() error {
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return nil
	}
	j.closed = true
	j.mu.Unlock()

	if j.syncMode == syncModeGroup && j.stopCh != nil {
		close(j.stopCh)
		<-j.doneCh // wait for the final fsync
	} else if j.f != nil {
		_ = j.f.Sync()
	}

	var firstErr error
	if j.f != nil {
		if err := j.f.Close(); err != nil {
			firstErr = err
		}
	}
	if j.lockF != nil {
		if err := j.lockF.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
