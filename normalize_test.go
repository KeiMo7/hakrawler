package main

import "testing"

func TestNormalizeURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"fragment stripped", "https://example.com/a#section", "https://example.com/a"},
		{"param order ignored", "https://example.com/a?b=2&a=1", "https://example.com/a?a=1&b=2"},
		{"param order reversed equal", "https://example.com/a?a=1&b=2", "https://example.com/a?b=2&a=1"},
		{"host lowercased", "https://EXAMPLE.com/A", "https://example.com/A"},
		{"empty fragment only", "https://example.com/a#", "https://example.com/a"},
		{"no query", "https://example.com/path/to", "https://example.com/path/to"},
		{"trailing slash significant", "https://example.com/a/", "https://example.com/a/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeURL(tc.in)
			if err != nil {
				t.Fatalf("normalizeURL(%q) error: %v", tc.in, err)
			}
			wantNorm, _ := normalizeURL(tc.want)
			if got != wantNorm {
				t.Errorf("normalizeURL(%q) = %q, want %q", tc.in, got, wantNorm)
			}
		})
	}

	// Different spellings must share one identity (acceptance criterion 3).
	a, _ := normalizeURL("https://example.com/page?z=9&a=1#frag")
	b, _ := normalizeURL("https://example.com/page?a=1&z=9#other")
	if a != b {
		t.Errorf("expected same identity, got %q vs %q", a, b)
	}

	// Invalid inputs.
	for _, bad := range []string{"", "/relative/path", "not a url"} {
		if _, err := normalizeURL(bad); err == nil {
			t.Errorf("normalizeURL(%q) expected error", bad)
		}
	}
}

func TestSameIdentity(t *testing.T) {
	if !sameIdentity("http://x.com/a?b=1&a=2#f1", "http://x.com/a?a=2&b=1#f2") {
		t.Error("equivalent spellings not recognized as same identity")
	}
	if sameIdentity("http://x.com/a", "http://x.com/b") {
		t.Error("different paths wrongly treated as same identity")
	}
}
