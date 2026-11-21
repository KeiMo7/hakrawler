package main

import "testing"

func TestNormalizeURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
		ok   bool
	}{
		{"fragment stripped", "http://example.com/a#frag", "http://example.com/a", true},
		{"query order", "http://example.com/a?b=2&a=1", "http://example.com/a?a=1&b=2", true},
		{"relative rejected", "/path/only", "/path/only", false},
		{"whitespace trimmed", "  http://example.com/x  ", "http://example.com/x", true},
		{"default http port stripped", "http://example.com:80/a", "http://example.com/a", true},
		{"default https port stripped", "https://example.com:443/a", "https://example.com/a", true},
		{"non-default port kept", "https://example.com:8443/a", "https://example.com:8443/a", true},
		{"uppercase host lowercased", "http://EXAMPLE.com/A", "http://example.com/A", true},
		{"uppercase scheme lowercased", "HTTP://example.com/", "http://example.com/", true},
		{"path case preserved", "http://example.com/CaseSensitive", "http://example.com/CaseSensitive", true},
		{"repeated params sorted", "http://example.com/?a=2&a=1", "http://example.com/?a=1&a=2", true},
		{"ipv6 kept", "http://[2001:db8::1]:8080/x", "http://[2001:db8::1]:8080/x", true},
		{"trailing slash kept", "http://example.com/a/", "http://example.com/a/", true},
		{"encoded query value preserved", "http://example.com/?q=hello%20world", "http://example.com/?q=hello+world", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := NormalizeURL(tc.in)
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v", ok, tc.ok)
			}
			if got != tc.want {
				t.Fatalf("NormalizeURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNormalizeEquivalenceGroup(t *testing.T) {
	// Acceptance: different writings of the same resource canonicalise to
	// one string (fragment, parameter order, host case, default port).
	variants := []string{
		"http://example.com/product?id=42&ref=home",
		"http://example.com/product?ref=home&id=42",
		"http://example.com/product?id=42&ref=home#section",
		"http://EXAMPLE.com/product?id=42&ref=home",
		"http://example.com:80/product?id=42&ref=home",
	}
	first, ok := NormalizeURL(variants[0])
	if !ok {
		t.Fatal("first variant must parse")
	}
	for _, v := range variants[1:] {
		got, ok := NormalizeURL(v)
		if !ok {
			t.Fatalf("variant %q failed to parse", v)
		}
		if got != first {
			t.Errorf("canonical mismatch:\n %q -> %q\n %q -> %q", variants[0], first, v, got)
		}
	}
}

func newTestStats() *Stats {
	s := &Stats{RuleHits: map[string]int64{}}
	return s
}

func TestGuardClassifyDepth(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeHost, "", nil, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://example.com/"); !ok {
		t.Fatal("seed rejected")
	}
	g.MarkSeeded("http://example.com/")

	// Depth 2 link is fine; depth 3 (> max depth 2) must be refused and
	// counted BEFORE any request could be created for it.
	if d, _, _ := g.Classify("http://example.com/l2", 2); d != DecisionEnqueue {
		t.Fatalf("depth 2 link: got decision %d, want enqueue", d)
	}
	d, _, _ := g.Classify("http://example.com/l3", 3)
	if d != DecisionDepth {
		t.Fatalf("depth 3 link: got decision %d, want depth", d)
	}
	if got := stats.DepthSkipped.Load(); got != 1 {
		t.Fatalf("depth_skipped=%d want 1", got)
	}
}

func TestGuardClassifyScope(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeHost, "", nil, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://example.com/"); !ok {
		t.Fatal("seed rejected")
	}
	g.MarkSeeded("http://example.com/")

	d, _, _ := g.Classify("http://evil.com/x", 2)
	if d != DecisionScope {
		t.Fatalf("off-host link: got %d want scope", d)
	}
	if stats.ScopeExcluded.Load() != 1 {
		t.Fatal("scope exclusion not counted")
	}
}

func TestGuardScopeDomain(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeDomain, "", nil, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://www.example.com/"); !ok {
		t.Fatal("seed rejected")
	}
	g.MarkSeeded("http://www.example.com/")

	if d, _, _ := g.Classify("http://api.example.com/v1", 2); d != DecisionEnqueue {
		t.Fatalf("subdomain should be in scope, got %d", d)
	}
	if d, _, _ := g.Classify("http://example.org/x", 2); d != DecisionScope {
		t.Fatalf("other registrable domain should be out of scope, got %d", d)
	}
	// public suffix boundary: example.com is eTLD+1, notexample.com is not
	// a subdomain of it.
	if d, _, _ := g.Classify("http://notexample.com/x", 2); d != DecisionScope {
		t.Fatalf("suffix-lookalike host should be out of scope, got %d", d)
	}
}

func TestGuardScopeWhitelist(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeWhitelist, "", []string{"allowed.test", "cdn.allowed.test"}, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://allowed.test/"); !ok {
		t.Fatal("seed rejected")
	}
	g.MarkSeeded("http://allowed.test/")

	if d, _, _ := g.Classify("http://cdn.allowed.test/a.js", 2); d != DecisionRule {
		// static-asset rule fires before identity but after scope; scope
		// itself must admit the host.
		t.Fatalf("whitelisted subdomain should pass scope, got %d", d)
	}
	if d, _, _ := g.Classify("http://unlisted.test/page", 2); d != DecisionScope {
		t.Fatalf("non-whitelisted host should be excluded, got %d", d)
	}
}

func TestGuardClassifyRuleHits(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeHost, "", nil, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://example.com/"); !ok {
		t.Fatal("seed rejected")
	}
	g.MarkSeeded("http://example.com/")

	cases := map[string]string{
		"http://example.com/app.js":        "static-asset",
		"http://example.com/img/logo.PNG":  "static-asset",
		"http://example.com/style.css?v=1": "static-asset",
		"http://example.com/down.zip":      "static-asset",
		"mailto:user@example.com":          "non-http-scheme",
		"javascript:alert(1)":              "non-http-scheme",
	}
	for link, wantRule := range cases {
		d, _, name := g.Classify(link, 2)
		if d != DecisionRule {
			t.Errorf("%s: decision=%d want rule", link, d)
		}
		if name != wantRule {
			t.Errorf("%s: rule=%s want %s", link, name, wantRule)
		}
	}
	if stats.RuleExcluded.Load() != int64(len(cases)) {
		t.Fatalf("rule_excluded=%d want %d", stats.RuleExcluded.Load(), len(cases))
	}
	if stats.RuleHits["static-asset"] != 4 {
		t.Fatalf("static-asset hits=%d want 4", stats.RuleHits["static-asset"])
	}
	// junk-path covers obviously meaningless navigation targets.
	if d, _, name := g.Classify("http://example.com/void(0)", 2); d != DecisionRule || name != "junk-path" {
		t.Fatalf("junk-path not matched: decision=%d rule=%s", d, name)
	}
}

func TestGuardClassifyDuplicate(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeHost, "", nil, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://example.com/"); !ok {
		t.Fatal("seed rejected")
	}
	g.MarkSeeded("http://example.com/")

	if d, _, _ := g.Classify("http://example.com/page?a=1&b=2", 2); d != DecisionEnqueue {
		t.Fatal("first occurrence must enqueue")
	}
	// Same resource, different writing: normalisation makes this a duplicate.
	if d, _, _ := g.Classify("http://example.com/page?b=2&a=1#x", 2); d != DecisionDuplicate {
		t.Fatal("canonical duplicate not detected")
	}
	if stats.DuplicateSkipped.Load() != 1 {
		t.Fatal("duplicate not counted")
	}
}

func TestGuardClassifyInvalid(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeHost, "", nil, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://example.com/"); !ok {
		t.Fatal("seed rejected")
	}
	g.MarkSeeded("http://example.com/")

	if d, _, _ := g.Classify("::not a url::", 2); d != DecisionInvalid {
		t.Fatalf("got %d want invalid", d)
	}
}

func TestStatsInvariantBucketsSumToCandidates(t *testing.T) {
	// Acceptance 五.3: the four reported numbers plus the duplicate bucket
	// (which the spec's four-number summary treats as part of the identity
	// story) must sum to the candidate total.
	stats := newTestStats()
	g := NewGuard(3, ScopeHost, "", nil, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://example.com/"); !ok {
		t.Fatal("seed rejected")
	}
	g.MarkSeeded("http://example.com/")

	links := []string{
		"http://example.com/a",          // enqueue
		"http://example.com/a?x=1",      // enqueue (different resource)
		"http://example.com/a?x=1#f",    // duplicate of above
		"http://example.com/deep1",      // enqueue at depth 2
		"http://offsite.com/x",          // scope
		"http://example.com/app.js",     // rule
		"http://example.com/deep4-link", // depth (classified at 4)
		"http://example.com/a?x=1&z=2",  // enqueue
	}
	for i, l := range links {
		depth := 2
		if i == 6 {
			depth = 4
		}
		g.Classify(l, depth)
	}

	snap := stats.Snapshot()
	sum := snap.Enqueued + snap.DepthSkipped + snap.ScopeExcluded +
		snap.RuleExcluded + snap.DuplicateSkipped
	if sum != snap.Candidates {
		t.Fatalf("invariant violated: buckets=%d candidates=%d (%+v)", sum, snap.Candidates, snap)
	}
	if snap.Extracted != snap.Candidates+snap.InvalidLinks {
		t.Fatalf("extracted=%d != candidates %d + invalid %d",
			snap.Extracted, snap.Candidates, snap.InvalidLinks)
	}
}

func TestSeedRejectedAtStartup(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeWhitelist, "", []string{"only.example"}, BuiltinRules(), stats)
	ok, reason := g.AddSeed("http://evil.example/start")
	if ok {
		t.Fatal("out-of-scope seed must be refused at startup")
	}
	if reason == "" {
		t.Fatal("refusal must explain why")
	}

	// Host-mode seed on a different host is likewise refused.
	g2 := NewGuard(2, ScopeHost, "", nil, BuiltinRules(), newTestStats())
	if ok, _ := g2.AddSeed("not-a-url"); ok {
		t.Fatal("unparseable seed must be refused")
	}
}

func TestInsidePathScope(t *testing.T) {
	stats := newTestStats()
	g := NewGuard(2, ScopeHost, "/blog", nil, BuiltinRules(), stats)
	if ok, _ := g.AddSeed("http://example.com/blog"); !ok {
		t.Fatal("seed rejected")
	}
	if !g.InScope("http://example.com/blog/post/1") {
		t.Fatal("path-prefix descendant must be in scope")
	}
	if g.InScope("http://example.com/admin") {
		t.Fatal("sibling path must be out of scope")
	}
}

func TestParseUserRules(t *testing.T) {
	rules, err := ParseUserRules([]string{"tracker=.*utm_", "bare-regex"})
	if err != nil {
		t.Fatal(err)
	}
	if rules[0].Name != "tracker" || !rules[0].Pattern.MatchString("http://x/?utm_source=a") {
		t.Fatal("named rule miscompiled")
	}
	// The bare regex is the second entry, hence the 1-based generated suffix.
	if rules[1].Name != "user-rule-2" {
		t.Fatalf("bare regex name=%s want user-rule-2", rules[1].Name)
	}
	if _, err := ParseUserRules([]string{"bad=["}); err == nil {
		t.Fatal("invalid regex must error")
	}
}
