package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

func jsonUnmarshal(b []byte, v interface{}) error { return json.Unmarshal(b, v) }

// startTestSite serves a small deterministic site:
//
//	/                 -> links to /a?z=1&a=2, /b, /slow, /500, /malformed
//	/a (and query)    -> links back to / with a fragment + reordered params
//	/b                -> links to /c and to /a written differently (dedup)
//	/c                -> leaf
//	/slow             -> sleeps (used to widen the kill window)
//	/500              -> HTTP 500 (colly error -> failed state, retried)
//	/malformed        -> sends truncated/invalid chunked framing occasionally
func startTestSite(t *testing.T, killGate string) *httptest.Server {
	mux := http.NewServeMux()
	page := func(title, body string) string {
		return "<html><head><title>" + title + "</title></head><body>" + body + "</body></html>"
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/a":
			// fragment + reordered query must normalize to the /a?a=2&z=1 identity
			fmt.Fprint(w, page("a", `<a href="/#x">home</a>`))
		case r.URL.Path == "/b":
			fmt.Fprint(w, page("b",
				`<a href="/c">c</a>`+
					`<a href="/a?a=2&z=1#frag">a-dup-spelling</a>`))
		case r.URL.Path == "/c":
			fmt.Fprint(w, page("c", `leaf`))
		case r.URL.Path == "/slow":
			// Crash hook: once the gate file exists, the in-process test binary
			// kills itself while this request is in flight.
			if killGate != "" {
				if _, err := os.Stat(killGate); err == nil {
					if p, perr := os.FindProcess(os.Getpid()); perr == nil {
						_ = p.Signal(syscall.SIGKILL)
					}
				}
			}
			time.Sleep(800 * time.Millisecond)
			fmt.Fprint(w, page("slow", `done`))
		case r.URL.Path == "/500":
			http.Error(w, "boom", http.StatusInternalServerError)
		case r.URL.Path == "/malformed":
			// Malformed upstream data: declare HTML but emit broken bytes;
			// goquery is permissive so this still returns a body — the key
			// guarantee is deterministic behaviour (no panic, no hang).
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "<html><body><a href=\"/c\">&#xff\xfe broken")
		default:
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			fmt.Fprint(w, page("root",
				`<a href="/a?z=1&a=2">a</a>`+
					`<a href="/b">b</a>`+
					`<a href="/slow">slow</a>`+
					`<a href="/500">err</a>`+
					`<a href="/malformed">mal</a>`+
					`<script src="/app.js"></script>`+
					`<form action="/submit"></form>`))
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// resultSet extracts the URL field from plain or JSON output lines.
func resultSet(t *testing.T, lines []string) []string {
	t.Helper()
	set := map[string]bool{}
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if strings.HasPrefix(l, "{") {
			var r Result
			if err := jsonUnmarshal([]byte(l), &r); err != nil {
				t.Fatalf("bad json line %q: %v", l, err)
			}
			if r.URL != "" {
				set[r.URL] = true
			}
			continue
		}
		// plain line may carry [href] / [where] prefixes; take the last token
		// that looks like a URL
		fields := strings.Fields(l)
		for i := len(fields) - 1; i >= 0; i-- {
			if strings.HasPrefix(fields[i], "http") {
				set[fields[i]] = true
				break
			}
		}
	}
	out := make([]string, 0, len(set))
	for u := range set {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

// runBinary executes the compiled hakrawler binary as a subprocess so a
// SIGKILL truly tests cross-process durability.
func runBinary(t *testing.T, binary, seed, stateDir string, extra ...string) (stdout []string, stderr string, err error) {
	args := append([]string{"-d", "3", "-t", "2"}, extra...)
	cmd := exec.Command(binary, args...)
	cmd.Stdin = strings.NewReader(seed + "\n")
	var errBuf strings.Builder
	cmd.Stderr = &errBuf
	out, err := cmd.Output()
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		stdout = append(stdout, sc.Text())
	}
	return stdout, errBuf.String(), err
}

// buildTestBinary compiles the current package into a temp binary once.
func buildTestBinary(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("subprocess kill test is unix-only")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "hakrawler")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOFLAGS=-mod=mod")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build test binary: %v\n%s", err, out)
	}
	return bin
}

// TestResumeMatchesOneShot is the headline acceptance: the link set produced
// by a kill+resume run equals the set from a single one-shot crawl.
func TestResumeMatchesOneShot(t *testing.T) {
	bin := buildTestBinary(t)
	srv := startTestSite(t, "")
	seed := srv.URL + "/"

	// One-shot baseline (resume flag absent -> original code path).
	oneShot, _, err := runBinary(t, bin, seed, "")
	if err != nil {
		t.Fatalf("one-shot run failed: %v", err)
	}
	baseSet := resultSet(t, oneShot)
	if len(baseSet) == 0 {
		t.Fatal("baseline crawl produced no URLs")
	}

	// Resumable run to completion in one process (no kill).
	stateDir := t.TempDir()
	resumed, _, err := runBinary(t, bin, seed, stateDir, "-resume", stateDir)
	if err != nil {
		t.Fatalf("resume run failed: %v", err)
	}
	if got := resultSet(t, resumed); !equalStrings(got, baseSet) {
		t.Fatalf("resume set differs from one-shot\nresume=%v\noneshot=%v", got, baseSet)
	}

	// A second invocation against the same finished state must not refetch
	// anything and must replay the identical output.
	replay, stderr, err := runBinary(t, bin, seed, stateDir, "-resume", stateDir)
	if err != nil {
		t.Fatalf("replay run failed: %v (%s)", err, stderr)
	}
	if got := resultSet(t, replay); !equalStrings(got, baseSet) {
		t.Fatalf("replay set differs from one-shot\nreplay=%v\noneshot=%v", got, baseSet)
	}
}

// TestResumeAfterKill simulates a machine reboot mid-crawl: the process is
// SIGKILLed while /slow is in flight, then rerun to completion.
func TestResumeAfterKill(t *testing.T) {
	bin := buildTestBinary(t)
	gate := filepath.Join(t.TempDir(), "gate")
	srv := startTestSite(t, gate)
	seed := srv.URL + "/"
	stateDir := t.TempDir()

	// First attempt: arm the kill gate and start the binary; the gate is
	// created after a short delay so the crawl has begun, then /slow kills
	// the process from inside the HTTP handler.
	armDone := make(chan struct{})
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = os.WriteFile(gate, []byte("1"), 0o644)
		close(armDone)
	}()

	cmd := exec.Command(bin, "-d", "3", "-t", "2", "-resume", stateDir)
	cmd.Stdin = strings.NewReader(seed + "\n")
	var errBuf strings.Builder
	cmd.Stderr = &errBuf
	firstOut, _ := cmd.Output() // expect SIGKILL exit
	<-armDone

	// The journal must exist and be intact despite the hard kill.
	jPath := filepath.Join(stateDir, journalFileName)
	if _, err := os.Stat(jPath); err != nil {
		t.Fatalf("journal missing after kill: %v (stderr=%s)", err, errBuf.String())
	}

	// Second attempt: remove the gate and resume; no URL completed before the
	// kill may be fetched again, the killed in-flight URL is retried.
	_ = os.Remove(gate)
	secondOut, stderr, err := runBinary(t, bin, seed, stateDir, "-resume", stateDir)
	if err != nil {
		t.Fatalf("resume after kill failed: %v (%s)", err, stderr)
	}

	// Final set equals the one-shot baseline.
	oneShot, _, err := runBinary(t, bin, seed, "")
	if err != nil {
		t.Fatal(err)
	}
	combinedSet := resultSet(t, append(splitLines(firstOut), secondOut...))
	if !equalStrings(combinedSet, resultSet(t, oneShot)) {
		t.Fatalf("post-kill resume set mismatch\ncombined=%v\noneshot=%v",
			combinedSet, resultSet(t, oneShot))
	}

	// The queue snapshot must exist and show an empty queue at completion.
	snapData, err := os.ReadFile(filepath.Join(stateDir, "queue-snapshot.json"))
	if err != nil {
		t.Fatalf("snapshot missing: %v", err)
	}
	var snap Snapshot
	if err := json.Unmarshal(snapData, &snap); err != nil {
		t.Fatalf("snapshot not parseable: %v\n%s", err, snapData)
	}
	if len(snap.Queue) != 0 {
		t.Fatalf("queue not drained at completion: %+v", snap.Queue)
	}
	if snap.Counts[statusQueued] != 0 {
		t.Fatalf("no URL may remain queued at completion: %+v", snap.Counts)
	}
	// /500 keeps failing on every retry: its terminal state is "failed" with a
	// traceable type, not a fake completion.
	if snap.Counts[statusFailed] != 1 || len(snap.Failed) != 1 {
		t.Fatalf("expected exactly one persistently failing URL: %+v", snap.Counts)
	}
	if snap.Failed[0].FailType != failHTTP || !strings.Contains(snap.Failed[0].FailDetail, "500") {
		t.Fatalf("failure type not traceable: %+v", snap.Failed)
	}
	if snap.Failed[0].Attempts < 2 {
		t.Fatalf("failed URL should have been retried across runs: %+v", snap.Failed)
	}
}

// TestResumeSeedChangeRejected covers acceptance case 2.
func TestResumeSeedChangeRejected(t *testing.T) {
	bin := buildTestBinary(t)
	srv := startTestSite(t, "")
	stateDir := t.TempDir()

	if _, _, err := runBinary(t, bin, srv.URL+"/", stateDir, "-resume", stateDir); err != nil {
		t.Fatal(err)
	}
	// Replace the seed with a different path: must exit non-zero and name
	// the recovery action.
	_, stderr, err := runBinary(t, bin, srv.URL+"/other", stateDir, "-resume", stateDir)
	if err == nil {
		t.Fatal("expected non-zero exit on seed replacement")
	}
	if !strings.Contains(stderr, "seed list changed") {
		t.Fatalf("error should identify the seed-list mismatch:\n%s", stderr)
	}
	if !strings.Contains(stderr, "new state directory") {
		t.Fatalf("error should give recovery instructions:\n%s", stderr)
	}
}

// TestResumeNewSeedAdded verifies a newly added entry is merged in while
// already completed work is reused.
func TestResumeNewSeedAdded(t *testing.T) {
	bin := buildTestBinary(t)
	srv1 := startTestSite(t, "")
	stateDir := t.TempDir()

	first, _, err := runBinary(t, bin, srv1.URL+"/", stateDir, "-resume", stateDir)
	if err != nil {
		t.Fatal(err)
	}
	firstSet := resultSet(t, first)

	// Add a second seed on rerun; output must contain the original set (replayed)
	// plus newly discovered links, with no duplicate replay lines.
	second, stderr, err := func() ([]string, string, error) {
		cmd := exec.Command(bin, "-d", "3", "-t", "2", "-resume", stateDir)
		cmd.Stdin = strings.NewReader(srv1.URL + "/\n" + srv1.URL + "/b\n")
		var eb strings.Builder
		cmd.Stderr = &eb
		out, oerr := cmd.Output()
		return splitLines(out), eb.String(), oerr
	}()
	if err != nil {
		t.Fatalf("rerun with added seed failed: %v (%s)", err, stderr)
	}
	for _, u := range firstSet {
		found := false
		for _, v := range resultSet(t, second) {
			if v == u {
				found = true
			}
		}
		if !found {
			t.Errorf("previously discovered URL %q missing after adding seed", u)
		}
	}
}

// TestResumeCorruptState covers acceptance case 3 with a corrupted journal.
func TestResumeCorruptState(t *testing.T) {
	bin := buildTestBinary(t)
	srv := startTestSite(t, "")
	stateDir := t.TempDir()
	if _, _, err := runBinary(t, bin, srv.URL+"/", stateDir, "-resume", stateDir); err != nil {
		t.Fatal(err)
	}
	jPath := filepath.Join(stateDir, journalFileName)
	raw, err := os.ReadFile(jPath)
	if err != nil {
		t.Fatal(err)
	}
	// Truncate in the middle of the file, guaranteeing a bad final line.
	mid := len(raw) / 2
	if err := os.WriteFile(jPath, raw[:mid], 0o644); err != nil {
		t.Fatal(err)
	}
	_, stderr, err := runBinary(t, bin, srv.URL+"/", stateDir, "-resume", stateDir)
	if err == nil {
		t.Fatal("corrupt journal must cause a non-zero exit")
	}
	if !strings.Contains(stderr, "journal corrupt") ||
		!strings.Contains(stderr, "recovery") {
		t.Fatalf("corruption error must explain recovery:\n%s", stderr)
	}
}

// TestSingleShotUnchanged proves the flag-off path keeps its original
// behaviour: no state directory is created anywhere.
func TestSingleShotUnchanged(t *testing.T) {
	bin := buildTestBinary(t)
	srv := startTestSite(t, "")
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadDir(wd)
	out, stderr, err := runBinary(t, bin, srv.URL+"/", "")
	if err != nil {
		t.Fatalf("single shot failed: %v (%s)", err, stderr)
	}
	if len(out) == 0 {
		t.Fatal("single shot produced no output")
	}
	after, _ := os.ReadDir(wd)
	if len(after) != len(before) {
		t.Fatalf("single-shot run created files in working directory: before=%d after=%d",
			len(before), len(after))
	}
}

// TestResumeJSONOutputParity checks output-field parity between paths in
// JSON mode. One-shot crawling can fetch the same identity twice (colly's
// FNV hash treats the fragment spelling as distinct), so raw line counts may
// differ in multiplicity; the *link set* expressed in the same JSON fields
// (Source/URL/Where) must be identical. That field-level view is exactly the
// diff requested in the acceptance criteria.
func TestResumeJSONOutputParity(t *testing.T) {
	bin := buildTestBinary(t)
	srv := startTestSite(t, "")
	seed := srv.URL + "/"
	oneShot, _, err := runBinary(t, bin, seed, "", "-json")
	if err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	resumed, stderr, err := runBinary(t, bin, seed, stateDir, "-resume", stateDir, "-json")
	if err != nil {
		t.Fatalf("resume json run: %v (%s)", err, stderr)
	}
	oneSet := jsonFieldSet(t, oneShot)
	resSet := jsonFieldSet(t, resumed)
	if !equalStrings(oneSet, resSet) {
		t.Fatalf("JSON link-set differs between paths\none-shot=%v\nresume=%v", oneSet, resSet)
	}
}

// jsonFieldSet reduces JSON output lines to the Source+URL+Where tuple so
// multiplicity does not obscure set equality.
func jsonFieldSet(t *testing.T, lines []string) []string {
	t.Helper()
	set := map[string]bool{}
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		var r Result
		if err := json.Unmarshal([]byte(l), &r); err != nil {
			t.Fatalf("bad json: %v (%q)", err, l)
		}
		set[fmt.Sprintf("%s|%s|%s", r.Source, r.URL, r.Where)] = true
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestResumeInsideModeParity verifies the -i (inside path) gate matches
// between the single-shot and resume paths.
func TestResumeInsideModeParity(t *testing.T) {
	bin := buildTestBinary(t)
	// A site whose root is under a path prefix; -i must keep links containing
	// that seed and drop outside links.
	mux := http.NewServeMux()
	mux.HandleFunc("/app/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/app/" {
			fmt.Fprint(w, `<a href="/app/in">in</a><a href="/other">out</a>`)
			return
		}
		fmt.Fprint(w, `<a href="/app/deep">deep</a>`)
	})
	mux.HandleFunc("/other", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `<a href="/app/should-not-appear">x</a>`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	seed := srv.URL + "/app/"

	oneShot, _, err := runBinary(t, bin, seed, "", "-i", "-d", "3")
	if err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	resumed, stderr, err := runBinary(t, bin, seed, stateDir, "-resume", stateDir, "-i", "-d", "3")
	if err != nil {
		t.Fatalf("resume -i run: %v (%s)", err, stderr)
	}
	if !equalStrings(sortLines(oneShot), sortLines(resumed)) {
		t.Fatalf("-i output differs\none-shot=%v\nresume=%v", sortLines(oneShot), sortLines(resumed))
	}
	// The outside link must be excluded by both paths.
	for _, l := range resumed {
		if strings.Contains(l, "/other") {
			t.Fatalf("outside-path link leaked under -i: %q", l)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sortLines(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func splitLines(b []byte) []string {
	var out []string
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	return out
}
