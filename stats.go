package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// WriteStats emits the end-of-run report. The human-readable summary always
// goes to stderr so stdout keeps containing only crawl results in their
// original format (hard constraint 三.3 / 二.3). When statsFile is empty the
// JSON document is appended to the stderr summary instead of touching disk.
//
// File output is atomic: bytes go to a sibling temp file which is fsynced and
// then renamed, so a crash mid-write can never leave a half-written stats
// file behind.
func WriteStats(snap StatsSnapshot, statsFile string) error {
	if statsFile != "" {
		if err := writeAtomicJSON(statsFile, snap); err != nil {
			return err
		}
	}
	fmt.Fprintln(os.Stderr, renderSummary(snap))
	if statsFile == "" {
		bytes, err := json.MarshalIndent(snap, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, string(bytes))
	} else {
		fmt.Fprintf(os.Stderr, "stats written to %s\n", statsFile)
	}
	return nil
}

// writeAtomicJSON writes to <path>.tmp then renames over the target. The temp
// file lives in the same directory so the rename is on one filesystem and
// therefore atomic on POSIX.
func writeAtomicJSON(path string, snap StatsSnapshot) error {
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, ".hakrawler-stats-*")
	if err != nil {
		return fmt.Errorf("create stats temp file: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	bytes, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		cleanup()
		return err
	}
	if _, err := tmp.Write(bytes); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return nil
}

// renderSummary builds the fixed-order, human-readable accounting block. Rule
// hits are sorted by descending count then name so "which rule fired most" is
// answered at a glance while remaining deterministic.
func renderSummary(snap StatsSnapshot) string {
	out := []byte("\n--- hakrawler boundary stats ---\n")
	add := func(format string, a ...any) {
		out = append(out, fmt.Sprintf(format, a...)...)
	}
	add("seeds accepted/rejected: %d / %d\n", snap.Seeds-snap.SeedsRejected, snap.SeedsRejected)
	add("links extracted (raw):   %d\n", snap.Extracted)
	add("  invalid links:         %d\n", snap.InvalidLinks)
	add("candidate links:          %d\n", snap.Candidates)
	add("  enqueued (crawled):    %d\n", snap.Enqueued)
	add("  depth-skipped:         %d\n", snap.DepthSkipped)
	add("  scope-excluded:        %d\n", snap.ScopeExcluded)
	add("  rule-excluded:         %d\n", snap.RuleExcluded)
	add("  duplicate-skipped:     %d\n", snap.DuplicateSkipped)
	add("page errors: fetch=%d parse=%d\n", snap.FetchErrors, snap.ParseErrors)
	if snap.TimedOut > 0 {
		add("timed-out seeds: %d\n", snap.TimedOut)
	}

	if len(snap.RuleHits) > 0 {
		type kv struct {
			name string
			hits int64
		}
		var rows []kv
		for k, v := range snap.RuleHits {
			rows = append(rows, kv{k, v})
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].hits != rows[j].hits {
				return rows[i].hits > rows[j].hits
			}
			return rows[i].name < rows[j].name
		})
		add("rule hits (most first):\n")
		for _, r := range rows {
			add("  %-16s %d\n", r.name, r.hits)
		}
	}

	// Invariant line: the four decision buckets plus duplicates partition
	// every candidate. Asserted in tests; printed so operators can verify it
	// on real runs too.
	bucketSum := snap.Enqueued + snap.DepthSkipped + snap.ScopeExcluded +
		snap.RuleExcluded + snap.DuplicateSkipped
	add("invariant enqueued+depth+scope+rule+duplicate = %d (candidates=%d)\n", bucketSum, snap.Candidates)
	return string(out)
}
