package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestJournal(t *testing.T) *Journal {
	t.Helper()
	dir := t.TempDir()
	// strict mode gives tests deterministic, Close()-independent durability
	j, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatalf("openJournal: %v", err)
	}
	t.Cleanup(func() { j.Close() })
	return j
}

func TestJournalSeedReconciliation(t *testing.T) {
	dir := t.TempDir()

	j, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	added, err := j.InitSeeds([]string{"https://a.test/"})
	if err != nil || len(added) != 1 {
		t.Fatalf("first InitSeeds: added=%v err=%v", added, err)
	}
	j.Close()

	// Reopen with an extra seed: it must be merged.
	j2, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	added, err = j2.InitSeeds([]string{"https://a.test/", "https://b.test/"})
	if err != nil || len(added) != 1 || added[0] != "https://b.test/" {
		t.Fatalf("added-seed merge: added=%v err=%v", added, err)
	}
	j2.Close()

	// Reopen with a replaced seed list (removal): must be rejected.
	j3, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = j3.InitSeeds([]string{"https://b.test/"})
	if !errors.Is(err, errSeedsChanged) {
		t.Fatalf("expected errSeedsChanged, got %v", err)
	}
	j3.Close()
}

func TestJournalStateTransitionsAndFailureTrace(t *testing.T) {
	j := newTestJournal(t)
	if _, err := j.InitSeeds([]string{"https://a.test/"}); err != nil {
		t.Fatal(err)
	}

	isNew, err := j.MarkQueued("https://a.test/p", 2, "https://a.test/")
	if err != nil || !isNew {
		t.Fatalf("first MarkQueued should be new: %v %v", isNew, err)
	}
	// Same identity with a fragment must not be enqueued twice.
	isNew, _ = j.MarkQueued("https://a.test/p#frag", 2, "https://a.test/")
	if isNew {
		t.Fatal("fragment variant must be recognized as the same identity")
	}
	// A different query string is a different identity.
	isNew, _ = j.MarkQueued("https://a.test/p?x=1", 2, "https://a.test/")
	if !isNew {
		t.Fatal("different query should be a different identity")
	}

	if err := j.MarkFailed("https://a.test/p", failTransport, "connection reset by peer"); err != nil {
		t.Fatal(err)
	}
	st, _ := j.StateOf("https://a.test/p")
	if st.Status != statusFailed || st.FailType != failTransport || st.Attempts != 1 {
		t.Fatalf("unexpected failed state: %+v", st)
	}

	// Failed URL must appear in Pending so the next run retries it.
	pending := j.Pending()
	found := false
	for _, q := range pending {
		if q.URL == "https://a.test/p" {
			found = true
		}
	}
	if !found {
		t.Fatal("failed URL not present in Pending (would not be retried)")
	}

	if err := j.MarkCompleted("https://a.test/p"); err != nil {
		t.Fatal(err)
	}
	st, _ = j.StateOf("https://a.test/p")
	if st.Status != statusCompleted || st.Attempts != 2 {
		t.Fatalf("expected completed after retry, got %+v", st)
	}
	for _, q := range j.Pending() {
		if q.URL == "https://a.test/p" {
			t.Fatal("completed URL must leave Pending")
		}
	}
}

func TestJournalReplayPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	j, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.InitSeeds([]string{"https://a.test/"}); err != nil {
		t.Fatal(err)
	}
	_, _ = j.MarkQueued("https://a.test/p1", 1, "https://a.test/")
	_ = j.MarkCompleted("https://a.test/p1")
	_, _ = j.RecordOutput("https://a.test/p1", "line-1")
	_, _ = j.RecordOutput("https://a.test/p2", "line-2")
	j.Close()

	j2, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer j2.Close()
	if got := j2.ReplayOutputs(); len(got) != 2 || got[0] != "line-1" || got[1] != "line-2" {
		t.Fatalf("replayed outputs wrong: %v", got)
	}
	st, ok := j2.StateOf("https://a.test/p1")
	if !ok || st.Status != statusCompleted {
		t.Fatalf("state did not survive reopen: %+v ok=%v", st, ok)
	}
}

func TestJournalCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	j, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.InitSeeds([]string{"https://a.test/"}); err != nil {
		t.Fatal(err)
	}
	_, _ = j.MarkQueued("https://a.test/p1", 1, "https://a.test/")
	_ = j.MarkCompleted("https://a.test/p1")
	_, _ = j.MarkQueued("https://a.test/p2", 1, "https://a.test/")
	j.Close()

	path := filepath.Join(dir, journalFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) < 3 {
		t.Fatalf("test setup: expected >=3 lines, got %d", len(lines))
	}

	// Corrupt a *middle* line by flipping a byte inside its JSON payload.
	bad := []byte(strings.Join(lines, "\n") + "\n")
	firstLineEnd := strings.IndexByte(string(bad), '\n')
	secondLineEnd := firstLineEnd + 1 + strings.IndexByte(string(bad[firstLineEnd+1:]), '\n')
	// mutate a byte safely inside the second line
	pos := firstLineEnd + 5
	orig := bad[pos]
	mut := orig
	for mut == orig {
		mut = (mut + 1) & 0x7f
	}
	bad[pos] = mut
	_ = secondLineEnd
	if err := os.WriteFile(path, bad, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = openJournal(dir, syncModeStrict, 0)
	if !errors.Is(err, errCorruptJournal) {
		t.Fatalf("expected errCorruptJournal, got: %v", err)
	}
	if !strings.Contains(err.Error(), "recovery") || !strings.Contains(err.Error(), ".corrupt") {
		t.Fatalf("error must explain recovery steps: %v", err)
	}
}

func TestJournalTornFinalLineRejected(t *testing.T) {
	dir := t.TempDir()
	j, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = j.InitSeeds([]string{"https://a.test/"})
	_, _ = j.MarkQueued("https://a.test/p1", 1, "https://a.test/")
	j.Close()

	path := filepath.Join(dir, journalFileName)
	raw, _ := os.ReadFile(path)
	// Append a partial line, as a kill mid-write would leave.
	if err := os.WriteFile(path, append(raw, []byte(`{"v":1,"crc":123,"kind":"sta`)...), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = openJournal(dir, syncModeStrict, 0)
	if !errors.Is(err, errCorruptJournal) {
		t.Fatalf("torn final line must be rejected, got: %v", err)
	}
}

func TestUnwritableStateDirRejected(t *testing.T) {
	parent := t.TempDir()
	blocked := filepath.Join(parent, "blocked")
	if err := os.MkdirAll(blocked, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o755) })
	err := assertStateDirWritable(filepath.Join(blocked, "state"))
	if err == nil {
		t.Fatal("expected unwritable state directory to be rejected")
	}
}

func TestGroupCommitDurability(t *testing.T) {
	dir := t.TempDir()
	// A very large window means records are only page-cache resident until
	// Close forces the final fsync.
	j, err := openJournal(dir, syncModeGroup, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.InitSeeds([]string{"https://a.test/"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		u := fmt.Sprintf("https://a.test/p%d", i)
		if _, err := j.MarkQueued(u, 1, "https://a.test/"); err != nil {
			t.Fatal(err)
		}
		_, _ = j.RecordOutput(u, "line-"+u)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: every record must have reached the file via the final fsync.
	j2, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	if got := len(j2.ReplayOutputs()); got != 50 {
		t.Fatalf("group commit lost records on clean close: got %d want 50", got)
	}
	if len(j2.Pending()) != 50 {
		t.Fatalf("queued state not durable: %d", len(j2.Pending()))
	}
}

func TestGroupCommitUnknownModeRejected(t *testing.T) {
	if _, err := openJournal(t.TempDir(), "bogus", 0); err == nil {
		t.Fatal("unknown sync mode must be rejected")
	}
}

func TestJournalCompaction(t *testing.T) {
	dir := t.TempDir()
	j, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = j.InitSeeds([]string{"https://a.test/"})
	// Generate enough history (multiple transitions per URL) to cross a
	// temporarily lowered threshold.
	oldThreshold := compactThresholdBytes
	compactThresholdBytes = 1
	t.Cleanup(func() { compactThresholdBytes = oldThreshold })

	for i := 0; i < 20; i++ {
		u := fmt.Sprintf("https://a.test/p%d", i)
		_, _ = j.MarkQueued(u, 1, "https://a.test/")
		_ = j.MarkFailed(u, failTransport, "x")
		_ = j.MarkCompleted(u)
		_, _ = j.RecordOutput(u, "line-"+u)
	}
	before, _ := os.Stat(filepath.Join(dir, journalFileName))
	if err := j.Compact(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(filepath.Join(dir, journalFileName))
	if after.Size() >= before.Size() {
		t.Fatalf("compaction did not shrink journal: before=%d after=%d", before.Size(), after.Size())
	}
	j.Close()

	// Every state and output must survive the rewrite.
	j2, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatalf("reopen after compact: %v", err)
	}
	defer j2.Close()
	if len(j2.Seeds()) != 1 {
		t.Fatalf("seeds lost in compaction: %v", j2.Seeds())
	}
	st, ok := j2.StateOf("https://a.test/p7")
	if !ok || st.Status != statusCompleted || st.Attempts != 2 {
		t.Fatalf("state/attempts lost in compaction: %+v ok=%v", st, ok)
	}
	if lines := j2.ReplayOutputs(); len(lines) != 20 {
		t.Fatalf("outputs lost in compaction: got %d want 20", len(lines))
	}
	if len(j2.Pending()) != 0 {
		t.Fatalf("compacted journal should have no pending work: %+v", j2.Pending())
	}
}

func TestSnapshotExport(t *testing.T) {
	dir := t.TempDir()
	j, err := openJournal(dir, syncModeStrict, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	_, _ = j.InitSeeds([]string{"https://a.test/"})
	_, _ = j.MarkQueued("https://a.test/q1", 1, "https://a.test/")
	_, _ = j.MarkQueued("https://a.test/q2", 2, "https://a.test/")
	_ = j.MarkCompleted("https://a.test/q1")
	_ = j.MarkFailed("https://a.test/q2", failHTTP, "HTTP 500")
	_, _ = j.RecordOutput("https://a.test/q1", "out-1")

	snapPath := filepath.Join(dir, "snap.json")
	if err := j.ExportSnapshot(snapPath); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	var snap Snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Counts[statusCompleted] != 1 || snap.Counts[statusFailed] != 1 {
		t.Fatalf("unexpected counts: %+v", snap.Counts)
	}
	// queue lists only still-queued items; failed addresses are reported
	// separately (with their failure type) even though both are retried.
	if len(snap.Queue) != 0 {
		t.Fatalf("queue should contain no queued items: %+v", snap.Queue)
	}
	if len(snap.Failed) != 1 || snap.Failed[0].URL != "https://a.test/q2" ||
		snap.Failed[0].FailType != failHTTP {
		t.Fatalf("failure trace missing/wrong: %+v", snap.Failed)
	}
	// Pending() must still include both classes so the runner retries.
	if len(j.Pending()) != 1 {
		t.Fatalf("failed URL must remain pending for retry: %+v", j.Pending())
	}
	if snap.OutputsRecorded != 1 {
		t.Fatalf("outputs recorded: %d", snap.OutputsRecorded)
	}

	// No tmp leftovers after atomic export.
	if _, err := os.Stat(snapPath + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("atomic export left a .tmp file")
	}
}
