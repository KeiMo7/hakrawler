package main

import (
	"net/url"
	"strings"
)

// normalizeURL returns the canonical identity of a URL.
//
// Identity rules (see README "Resumable crawling"):
//   - the URL must be absolute and parseable, otherwise an error is returned
//   - the fragment is dropped (it never reaches the server)
//   - query parameters are sorted by (key, value), so parameter order does
//     not produce a different identity
//   - host is lower-cased (net/url already does this)
//   - percent-encoding is left untouched apart from what url.Parse/String
//     normalise itself; the scheme and host case are normalised
//
// Two textual spellings of the same resource therefore map to one identity,
// which is what makes "the same address is only fetched once" testable.
func normalizeURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if !u.IsAbs() || u.Host == "" {
		return "", errNotAbsolute
	}

	u.Fragment = ""
	u.RawFragment = ""

	// net/url preserves the host's original case; normalise it explicitly so
	// https://Example.com and https://example.com share one identity.
	u.Host = strings.ToLower(u.Host)

	if u.RawQuery != "" {
		values := u.Query() // splits on order-independent key/value pairs
		u.RawQuery = values.Encode()
	}

	// Opaque URLs (mailto:, etc.) are out of scope for crawling.
	if u.Opaque != "" {
		return "", errNotAbsolute
	}

	return u.String(), nil
}

// sameIdentity reports whether two raw URL strings normalize to the same
// identity. Unparseable inputs compare on raw equality.
func sameIdentity(a, b string) bool {
	na, errA := normalizeURL(a)
	nb, errB := normalizeURL(b)
	if errA != nil || errB != nil {
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	}
	return na == nb
}
