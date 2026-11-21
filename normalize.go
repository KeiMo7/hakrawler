package main

import (
	"net/url"
	"sort"
	"strings"
)

// NormalizeURL is the single definition of URL canonicalisation used by every
// processing path (seed validation, enqueue guard, dedup). It must never be
// bypassed or duplicated elsewhere.
//
// Transformations applied, in order:
//  1. trim surrounding whitespace
//  2. parse as an absolute URL (relative URLs are returned with ok=false)
//  3. lowercase scheme and host (host casing is semantically insignificant)
//  4. drop the default port for the scheme (http:80, https:443)
//  5. strip the fragment (RFC 3986: fragments are not sent to the server,
//     so http://x/a#b and http://x/a are the same resource)
//  6. sort query parameters by key, then by value, preserving duplicates
//  7. rebuild the query string in the sorted order
//
// Deliberately NOT transformed:
//   - path case: paths are case-sensitive on most HTTP servers, so lowercasing
//     them would merge distinct resources. The "case" canonicalisation required
//     by the spec therefore applies to scheme/host only.
//   - percent-encoding, trailing slashes and dot segments: rewriting them can
//     change which resource a server returns, so they are left untouched.
//
// ok is false when the input cannot be parsed as an absolute URL; in that case
// raw is the trimmed input and the caller decides how to account for it.
func NormalizeURL(raw string) (canonical string, ok bool) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || !u.IsAbs() {
		return raw, false
	}

	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = normalizeHost(u.Scheme, u.Host)
	u.Fragment = ""
	u.RawFragment = ""

	if u.RawQuery != "" {
		values, err := url.ParseQuery(u.RawQuery)
		if err == nil {
			u.RawQuery = encodeSortedQuery(values)
		}
		// If the query cannot be parsed, keep it verbatim rather than
		// silently dropping data.
	}

	return u.String(), true
}

// normalizeHost lowercases the host and strips the default port for the
// scheme. url.URL.Host may contain a port; url.URL.Hostname() strips it but
// loses a non-default port, so the port is handled explicitly.
func normalizeHost(scheme, host string) string {
	lower := strings.ToLower(host)
	h := lower
	port := ""
	if i := strings.LastIndex(lower, ":"); i >= 0 {
		// A bare IPv6 literal without port contains no colon after the
		// closing bracket; LastIndex finds the port separator otherwise.
		if strings.HasPrefix(lower, "[") {
			if closeBracket := strings.Index(lower, "]"); closeBracket >= 0 && i > closeBracket {
				h, port = lower[:i], lower[i+1:]
			}
		} else {
			h, port = lower[:i], lower[i+1:]
		}
	}
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	if port != "" {
		return h + ":" + port
	}
	return h
}

// encodeSortedQuery serialises url.Values with keys sorted lexicographically
// and values sorted within each key, so "?b=2&a=1" and "?a=1&b=2" (and any
// permutation of repeated parameters) produce the same string.
func encodeSortedQuery(values url.Values) string {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		vs := append([]string(nil), values[k]...)
		sort.Strings(vs)
		for _, v := range vs {
			if sb.Len() > 0 {
				sb.WriteByte('&')
			}
			sb.WriteString(url.QueryEscape(k))
			sb.WriteByte('=')
			sb.WriteString(url.QueryEscape(v))
		}
	}
	return sb.String()
}
