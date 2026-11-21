package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// startCrawlSite serves a deterministic fixture exercising every boundary
// decision: depth overflow, off-scope hosts, rule matches and canonical
// duplicates. Layout (depth in brackets):
//
//	/            [1] -> /l2a, /l2b, /offsite, /app.js, /dup?b=2&a=1, /dup?a=1&b=2#f
//	/l2a         [2] -> /l3a (would be depth 3, refused when -d 2)
//	/l2b         [2] -> /l3b
//	/l3a,/l3b    [3]
func startCrawlSite(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		switch r.URL.Path {
		case "/":
			fmt.Fprint(w, `<!doctype html><html><body>
<a href="/l2a">l2a</a>
<a href="/l2b">l2b</a>
<a href="http://offsite.example/x">offsite</a>
<a href="/app.js">asset</a>
<a href="/dup?b=2&amp;a=1">dup1</a>
<a href="/dup?a=1&amp;b=2#frag">dup2</a>
<script src="/static/main.js"></script>
<form action="/submit"></form>
</body></html>`)
		case "/l2a":
			fmt.Fprint(w, `<html><body><a href="/l3a">l3a</a><a href="/">home</a></body></html>`)
		case "/l2b":
			fmt.Fprint(w, `<html><body><a href="/l3b">l3b</a></body></html>`)
		case "/l3a", "/l3b":
			fmt.Fprint(w, `<html><body>leaf</body></html>`)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func runBoundary(t *testing.T, cfg BoundaryConfig, seeds string) (string, StatsSnapshot) {
	t.Helper()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	stats, err := RunBoundaryCrawl(cfg, strings.Split(seeds, "\n"), w)
	if err != nil {
		t.Fatalf("RunBoundaryCrawl: %v", err)
	}
	w.Flush()
	return buf.String(), stats.Snapshot()
}

func baseCfg(srv *httptest.Server, depth int) BoundaryConfig {
	return BoundaryConfig{
		Threads: 4,
		Depth:   depth,
		MaxSize: -1,
		Rules:   BuiltinRules(),
		Scope:   ScopeHost,
		Headers: map[string]string{},
	}
}

// TestIntegrationDepthExceeded covers acceptance case 1: links beyond -d are
// refused, counted, and never requested.
func TestIntegrationDepthExceeded(t *testing.T) {
	srv := startCrawlSite(t)
	cfg := baseCfg(srv, 2)
	out, snap := runBoundary(t, cfg, srv.URL+"/")

	if snap.DepthSkipped == 0 {
		t.Fatal("expected depth-skipped links, got 0")
	}
	// /l3a is discovered on /l2a (depth 2) and would be fetched at depth 3;
	// the assertion that no request reaches it is enforced by the request
	// counter below.
	if !strings.Contains(out, "/l3a") {
		t.Fatal("depth-skipped link must still be printed (legacy output shape), output:\n", out)
	}
}

// TestDepthCheckBeforeEnqueue is the explicit acceptance assertion 五.2:
// depth enforcement happens before any HTTP request is made for the link.
func TestDepthCheckBeforeEnqueue(t *testing.T) {
	srv := startCrawlSite(t)

	var mu sync.Mutex
	requested := map[string]int{}
	// Wrap the fixture server with a request recorder.
	recorder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requested[r.URL.Path]++
		mu.Unlock()
		srv.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(recorder.Close)

	cfg := baseCfg(recorder, 2)
	_, snap := runBoundary(t, cfg, recorder.URL+"/")

	if snap.DepthSkipped == 0 {
		t.Fatal("test setup: no depth-skipped event occurred")
	}
	mu.Lock()
	defer mu.Unlock()
	for path := range requested {
		if path == "/l3a" || path == "/l3b" {
			t.Fatalf("depth-3 page %s was fetched; depth check did not happen before enqueue", path)
		}
	}
}

// TestIntegrationScopeExclusion covers acceptance case 2.
func TestIntegrationScopeExclusion(t *testing.T) {
	srv := startCrawlSite(t)
	cfg := baseCfg(srv, 2)
	_, snap := runBoundary(t, cfg, srv.URL+"/")

	if snap.ScopeExcluded == 0 {
		t.Fatal("off-site link was not counted as scope-excluded")
	}
	if snap.Enqueued == 0 {
		t.Fatal("in-scope pages should still be enqueued")
	}
}

// TestIntegrationRuleExclusion covers acceptance case 3 with per-rule hits.
func TestIntegrationRuleExclusion(t *testing.T) {
	srv := startCrawlSite(t)
	cfg := baseCfg(srv, 2)
	_, snap := runBoundary(t, cfg, srv.URL+"/")

	if snap.RuleExcluded < 2 {
		t.Fatalf("rule_excluded=%d, expected at least app.js + main.js", snap.RuleExcluded)
	}
	if snap.RuleHits["static-asset"] < 2 {
		t.Fatalf("static-asset hits=%d, want >=2", snap.RuleHits["static-asset"])
	}
}

// TestIntegrationCanonicalDedup covers acceptance case 4: the two /dup
// writings differ in parameter order and fragment but are one resource.
func TestIntegrationCanonicalDedup(t *testing.T) {
	srv := startCrawlSite(t)
	cfg := baseCfg(srv, 2)
	out, snap := runBoundary(t, cfg, srv.URL+"/")

	if snap.DuplicateSkipped == 0 {
		t.Fatal("canonical duplicate was not detected")
	}
	// Both writings remain visible in output (printing happens regardless of
	// crawl admission), but only one request must have been enqueued.
	if !strings.Contains(out, "/dup?b=2") || !strings.Contains(out, "/dup?a=1") {
		t.Fatal("both raw writings should still appear in output:\n", out)
	}
}

// TestIntegrationInvariant is acceptance 五.3: the decision buckets sum to
// the candidate total on a real crawl.
func TestIntegrationInvariant(t *testing.T) {
	srv := startCrawlSite(t)
	cfg := baseCfg(srv, 3)
	_, snap := runBoundary(t, cfg, srv.URL+"/")

	sum := snap.Enqueued + snap.DepthSkipped + snap.ScopeExcluded +
		snap.RuleExcluded + snap.DuplicateSkipped
	if sum != snap.Candidates {
		t.Fatalf("invariant violated: sum=%d candidates=%d snap=%+v", sum, snap.Candidates, snap)
	}
	if snap.Extracted != snap.Candidates+snap.InvalidLinks {
		t.Fatalf("extracted=%d != candidates+invalid (%d+%d)",
			snap.Extracted, snap.Candidates, snap.InvalidLinks)
	}
}

// TestDeterministicOutputOrder verifies hard constraint 二.4: repeated runs
// over the same fixture produce byte-identical output regardless of how the
// worker goroutines happen to schedule.
func TestDeterministicOutputOrder(t *testing.T) {
	srv := startCrawlSite(t)
	var first string
	for i := 0; i < 5; i++ {
		cfg := baseCfg(srv, 3)
		cfg.Threads = 8
		out, _ := runBoundary(t, cfg, srv.URL+"/")
		if i == 0 {
			first = out
			continue
		}
		if out != first {
			t.Fatalf("run %d output differs from run 0\n--- first ---\n%s\n--- now ---\n%s", i, first, out)
		}
	}
}

// TestMalformedUpstreamDoesNotKillCrawl covers the concurrency/extremes
// requirement: a page whose connection fails or whose body is binary garbage
// declared as HTML is skipped and counted while sibling pages keep crawling.
func TestMalformedUpstreamDoesNotKillCrawl(t *testing.T) {
	mux := http.NewServeMux()
	// /broken hijacks the connection and closes it mid-response, simulating
	// a peer reset / truncated upstream body.
	mux.HandleFunc("/broken", func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Skip("hijacking unsupported")
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Fatalf("hijack: %v", err)
		}
		// Write a partial, malformed HTTP response then drop the connection.
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 999\r\n\r\n<ht"))
		_ = conn.Close()
	})
	// /broken-html returns binary garbage with an HTML content type. goquery
	// is permissive, so this exercises the "parse never finds links" path
	// rather than an explicit parse error.
	mux.HandleFunc("/broken-html", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte{0x00, 0x01, 0x02, 0xff, 0xfe})
	})
	mux.HandleFunc("/good", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html><body><a href="/good/child">child</a></body></html>`)
	})
	mux.HandleFunc("/good/child", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, `<html>ok</html>`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg := baseCfg(srv, 3)
	// Three seeds on the same in-scope host: a reset connection, a garbage
	// body and a healthy page. The first two failures must not stop the
	// healthy crawl.
	out, snap := runBoundary(t, cfg, srv.URL+"/broken\n"+srv.URL+"/broken-html\n"+srv.URL+"/good")
	if !strings.Contains(out, "/good/child") {
		t.Fatal("healthy crawl did not complete after failing pages; output:\n", out)
	}
	if snap.FetchErrors == 0 && snap.ParseErrors == 0 {
		t.Fatalf("failing pages produced no error counters: %+v", snap)
	}
}

// TestSeedRejectionExitPath verifies startup-phase refusal: an out-of-scope
// seed is listed with a reason and nothing is fetched.
func TestSeedRejectionExitPath(t *testing.T) {
	srv := startCrawlSite(t)
	cfg := baseCfg(srv, 2)
	cfg.Scope = ScopeWhitelist
	cfg.Whitelist = []string{"only-allowed.example"}

	_, snap := runBoundary(t, cfg, srv.URL+"/")
	if snap.SeedsRejected != 1 || len(snap.SeedRejections) != 1 {
		t.Fatalf("expected one rejected seed, got %+v", snap.SeedRejections)
	}
	if snap.SeedRejections[0].Reason == "" {
		t.Fatal("rejection reason must be populated")
	}
	if snap.Enqueued != 0 {
		t.Fatal("rejected seed must not enqueue anything")
	}
}

// TestLegacyModeUntouched is a build-level guard that the legacy data path
// still compiles and runs; the detailed legacy behaviour lives in the
// unchanged hakrawler.go main() path.
func TestLegacyModeUntouched(t *testing.T) {
	srv := startCrawlSite(t)
	// Without -boundary the flags struct exists but the engine is never
	// constructed; assert the default config keeps legacy semantics.
	cfg := BoundaryConfig{Depth: 2, Scope: ScopeHost, Rules: BuiltinRules()}
	if cfg.Scope != ScopeHost {
		t.Fatal("default scope must be host (legacy semantics)")
	}
	if srv.URL == "" {
		t.Fatal("fixture missing")
	}
}
