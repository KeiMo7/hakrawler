package main

import (
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/net/publicsuffix"
)

// Decision is the outcome of the pre-enqueue boundary check. Every candidate
// link falls into exactly one bucket; the buckets partition the candidate set,
// which is asserted by the test suite (invariant: the buckets sum to the
// total number of candidate links).
type Decision int

const (
	// DecisionEnqueue: the candidate passed every check and is crawled.
	DecisionEnqueue Decision = iota
	// DecisionDepth: the candidate's depth exceeds the configured limit.
	DecisionDepth
	// DecisionScope: the candidate's host is outside the configured scope.
	DecisionScope
	// DecisionRule: the candidate matched an exclusion rule.
	DecisionRule
	// DecisionDuplicate: the canonical URL has already been seen.
	DecisionDuplicate
	// DecisionInvalid: the candidate cannot be parsed as an absolute URL.
	DecisionInvalid
)

// ScopeMode selects how the allowed host set is derived from each seed.
type ScopeMode string

const (
	// ScopeHost: only the seed's exact host is in scope ("站内").
	ScopeHost ScopeMode = "host"
	// ScopeDomain: the seed's registrable domain (eTLD+1) and any of its
	// subdomains are in scope ("同主域").
	ScopeDomain ScopeMode = "domain"
	// ScopeWhitelist: only hosts supplied via -whitelist are in scope.
	ScopeWhitelist ScopeMode = "whitelist"
)

// ExclusionRule is one named exclusion pattern. Built-in rules and user
// rules share this type so rule-hit accounting is uniform.
type ExclusionRule struct {
	Name    string
	Pattern *regexp.Regexp
}

// Stats is the end-of-run accounting. Counter fields use atomic.Int64 so the
// hot path (one increment per extracted link) needs no mutex; the maps are
// only touched under mu. The struct is serialised once, after every worker
// has stopped, so the emitted JSON is deterministic regardless of scheduling.
type Stats struct {
	mu sync.Mutex `json:"-"`

	// Seeds is the number of non-empty seed lines read from stdin.
	Seeds atomic.Int64 `json:"seeds"`
	// SeedsRejected is the number of seeds refused at startup because they
	// fall outside the configured scope (hard constraint 三.2).
	SeedsRejected atomic.Int64 `json:"seeds_rejected"`
	// Extracted is the total number of raw links extracted from pages,
	// before normalisation.
	Extracted atomic.Int64 `json:"extracted_links"`
	// Candidates is the number of parseable links that entered the boundary
	// pipeline (InvalidLinks = Extracted - Candidates).
	Candidates atomic.Int64 `json:"candidate_links"`
	// Enqueued is how many candidates passed all checks and were crawled.
	Enqueued atomic.Int64 `json:"enqueued"`
	// DepthSkipped counts links refused because their depth exceeded -d.
	DepthSkipped atomic.Int64 `json:"depth_skipped"`
	// ScopeExcluded counts links refused by the scope check.
	ScopeExcluded atomic.Int64 `json:"scope_excluded"`
	// RuleExcluded counts links refused by an exclusion rule.
	RuleExcluded atomic.Int64 `json:"rule_excluded"`
	// DuplicateSkipped counts links whose canonical form was already seen.
	DuplicateSkipped atomic.Int64 `json:"duplicate_skipped"`
	// InvalidLinks counts extracted strings that could not be parsed as
	// absolute URLs.
	InvalidLinks atomic.Int64 `json:"invalid_links"`
	// ParseErrors counts pages whose HTML parsing failed (degraded, skipped).
	ParseErrors atomic.Int64 `json:"parse_errors"`
	// FetchErrors counts transport-level failures (reset, timeout, ...).
	FetchErrors atomic.Int64 `json:"fetch_errors"`
	// TimedOut is set when one or more seeds hit the -timeout budget.
	TimedOut atomic.Int64 `json:"timed_out_seeds"`
	// RuleHits breaks RuleExcluded down by rule name.
	RuleHits map[string]int64 `json:"rule_hits"`
	// PerSeed holds one entry per rejected seed explaining the refusal.
	PerSeed []SeedRejection `json:"seed_rejections,omitempty"`
}

// SeedRejection records why a seed was refused before any request was made.
type SeedRejection struct {
	Seed   string `json:"seed"`
	Reason string `json:"reason"`
}

// Snapshot returns a mutex-free, JSON-friendly plain copy of the counters
// taken at shutdown, with rule hits sorted for stable rendering.
type StatsSnapshot struct {
	Seeds            int64            `json:"seeds"`
	SeedsRejected    int64            `json:"seeds_rejected"`
	Extracted        int64            `json:"extracted_links"`
	Candidates       int64            `json:"candidate_links"`
	Enqueued         int64            `json:"enqueued"`
	DepthSkipped     int64            `json:"depth_skipped"`
	ScopeExcluded    int64            `json:"scope_excluded"`
	RuleExcluded     int64            `json:"rule_excluded"`
	DuplicateSkipped int64            `json:"duplicate_skipped"`
	InvalidLinks     int64            `json:"invalid_links"`
	ParseErrors      int64            `json:"parse_errors"`
	FetchErrors      int64            `json:"fetch_errors"`
	TimedOut         int64            `json:"timed_out_seeds"`
	RuleHits         map[string]int64 `json:"rule_hits"`
	SeedRejections   []SeedRejection  `json:"seed_rejections,omitempty"`
}

// Snapshot freezes the stats for serialisation and invariant checks.
func (s *Stats) Snapshot() StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	hits := make(map[string]int64, len(s.RuleHits))
	for k, v := range s.RuleHits {
		hits[k] = v
	}
	return StatsSnapshot{
		Seeds:            s.Seeds.Load(),
		SeedsRejected:    s.SeedsRejected.Load(),
		Extracted:        s.Extracted.Load(),
		Candidates:       s.Candidates.Load(),
		Enqueued:         s.Enqueued.Load(),
		DepthSkipped:     s.DepthSkipped.Load(),
		ScopeExcluded:    s.ScopeExcluded.Load(),
		RuleExcluded:     s.RuleExcluded.Load(),
		DuplicateSkipped: s.DuplicateSkipped.Load(),
		InvalidLinks:     s.InvalidLinks.Load(),
		ParseErrors:      s.ParseErrors.Load(),
		FetchErrors:      s.FetchErrors.Load(),
		TimedOut:         s.TimedOut.Load(),
		RuleHits:         hits,
		SeedRejections:   append([]SeedRejection(nil), s.PerSeed...),
	}
}

// Guard performs all pre-enqueue checks. A single Guard instance is shared by
// every worker goroutine of a run; the seen set and the counters are the only
// mutable state and both are concurrency-safe.
type Guard struct {
	depth      int
	scope      ScopeMode
	insidePath string // when -i is set, links must stay under this seed path prefix
	whitelist  map[string]bool
	// scopeHosts stores exact allowed hosts for ScopeHost.
	scopeHosts map[string]bool
	// scopeDomains stores eTLD+1 per seed for ScopeDomain.
	scopeDomains map[string]bool
	rules        []ExclusionRule
	seen         sync.Map // canonical URL -> struct{}
	stats        *Stats
}

// NewGuard builds the boundary guard from the resolved configuration.
func NewGuard(depth int, scope ScopeMode, insidePath string, whitelist []string, rules []ExclusionRule, stats *Stats) *Guard {
	g := &Guard{
		depth:        depth,
		scope:        scope,
		insidePath:   insidePath,
		whitelist:    map[string]bool{},
		scopeHosts:   map[string]bool{},
		scopeDomains: map[string]bool{},
		rules:        rules,
		stats:        stats,
	}
	for _, h := range whitelist {
		g.whitelist[strings.ToLower(strings.TrimSpace(h))] = true
	}
	return g
}

// AddSeed registers a seed's host so scope predicates can be derived. It
// returns false with a human-readable reason when the seed itself is outside
// the configured scope; the caller must refuse such seeds before crawling.
func (g *Guard) AddSeed(seedURL string) (bool, string) {
	u, err := url.Parse(strings.TrimSpace(seedURL))
	if err != nil || !u.IsAbs() {
		return false, "seed is not a valid absolute URL"
	}
	host := strings.ToLower(u.Hostname())

	switch g.scope {
	case ScopeWhitelist:
		if !g.hostAllowed(host) {
			return false, "seed host " + host + " is not in the -whitelist"
		}
	case ScopeDomain:
		reg, err := publicsuffix.EffectiveTLDPlusOne(host)
		if err != nil {
			return false, "seed host " + host + " has no registrable domain"
		}
		g.scopeDomains[reg] = true
	case ScopeHost:
		g.scopeHosts[host] = true
	}
	return true, ""
}

// Classify runs the full pre-enqueue pipeline against one extracted link and
// records the outcome in Stats. Checks are applied in a fixed order:
// depth -> non-HTTP-scheme rule -> scope (host/path) -> remaining shape rules
// -> dedup. The link's depth is the depth it *would* be fetched at (parent
// depth + 1), so a refusal here means no goroutine and no HTTP request is
// ever created for it.
//
// canonical is the normalised form (empty when ok is false).
func (g *Guard) Classify(rawLink string, linkDepth int) (decision Decision, canonical string, ruleName string) {
	g.stats.Extracted.Add(1)

	canonical, ok := NormalizeURL(rawLink)
	if !ok {
		g.stats.InvalidLinks.Add(1)
		return DecisionInvalid, "", ""
	}
	g.stats.Candidates.Add(1)

	if linkDepth > g.depth {
		g.stats.DepthSkipped.Add(1)
		return DecisionDepth, canonical, ""
	}
	// Scope first: a .js file on a foreign host is an out-of-scope exclusion,
	// not a static-asset rule hit. The only shape check that precedes scope
	// is the non-HTTP scheme, which inScope() rejects with no useful host
	// semantics; keeping it a rule gives the operator the precise reason.
	if name, matched := g.matchNonHTTPScheme(canonical); matched {
		g.stats.RuleExcluded.Add(1)
		g.recordRuleHit(name)
		return DecisionRule, canonical, name
	}
	if !g.inScope(canonical) {
		g.stats.ScopeExcluded.Add(1)
		return DecisionScope, canonical, ""
	}
	if name, matched := g.matchRule(canonical); matched {
		g.stats.RuleExcluded.Add(1)
		g.recordRuleHit(name)
		return DecisionRule, canonical, name
	}
	if _, loaded := g.seen.LoadOrStore(canonical, struct{}{}); loaded {
		g.stats.DuplicateSkipped.Add(1)
		return DecisionDuplicate, canonical, ""
	}

	g.stats.Enqueued.Add(1)
	return DecisionEnqueue, canonical, ""
}

// MarkSeeded registers a seed that passed validation in the dedup set so the
// same URL discovered later on a page is counted as a duplicate rather than
// enqueued twice.
func (g *Guard) MarkSeeded(canonical string) {
	g.seen.Store(canonical, struct{}{})
}

// InScope exposes the scope predicate for seed validation and tests without
// going through the counting pipeline.
func (g *Guard) InScope(rawURL string) bool {
	return g.inScope(rawURL)
}

func (g *Guard) inScope(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || !u.IsAbs() {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if !g.hostAllowed(host) {
		return false
	}
	// An empty insidePath means no path-prefix restriction; HasPrefix with
	// an empty prefix is always true anyway, the check makes the intent
	// explicit and guards against future callers.
	if g.insidePath != "" && !strings.HasPrefix(u.Path, g.insidePath) {
		return false
	}
	return true
}

// hostAllowed implements the three scope modes against a lowercased host.
// Whitelist entries match either exactly or as a domain suffix
// ("example.com" admits "api.example.com"), which mirrors how seeds expand
// under the domain mode.
func (g *Guard) hostAllowed(host string) bool {
	switch g.scope {
	case ScopeWhitelist:
		if g.whitelist[host] {
			return true
		}
		for allowed := range g.whitelist {
			if strings.HasSuffix(host, "."+allowed) {
				return true
			}
		}
		return false
	case ScopeDomain:
		reg, err := publicsuffix.EffectiveTLDPlusOne(host)
		if err != nil {
			return false
		}
		return g.scopeDomains[reg]
	default: // ScopeHost
		return g.scopeHosts[host]
	}
}

func (g *Guard) matchRule(canonical string) (string, bool) {
	for _, r := range g.rules {
		if r.Name == "non-http-scheme" {
			continue
		}
		if r.Pattern.MatchString(canonical) {
			return r.Name, true
		}
	}
	return "", false
}

// matchNonHTTPScheme isolates the scheme rule so it can be evaluated before
// the host-scope check (a mailto: has no in-scope host to test against).
func (g *Guard) matchNonHTTPScheme(canonical string) (string, bool) {
	for _, r := range g.rules {
		if r.Name == "non-http-scheme" && r.Pattern.MatchString(canonical) {
			return r.Name, true
		}
	}
	return "", false
}

func (g *Guard) recordRuleHit(name string) {
	g.stats.mu.Lock()
	defer g.stats.mu.Unlock()
	if g.stats.RuleHits == nil {
		g.stats.RuleHits = map[string]int64{}
	}
	g.stats.RuleHits[name]++
}

// RecordSeedRejection appends a structured refusal; the startup phase is
// single-threaded, the mutex keeps the type safe under any future caller.
func (s *Stats) RecordSeedRejection(seed, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SeedsRejected.Add(1)
	s.PerSeed = append(s.PerSeed, SeedRejection{Seed: seed, Reason: reason})
}

// BuiltinRules returns the default exclusion rule set. User-supplied rules
// (if any) are evaluated *before* these so custom names show up in the hits.
func BuiltinRules() []ExclusionRule {
	return []ExclusionRule{
		{
			// Non-HTTP navigation (mailto:, tel:, javascript:, ftp:// ...).
			// RE2 has no negative lookahead, so the non-http set is matched
			// explicitly; inScope() independently rejects non-http(s) URLs.
			Name:    "non-http-scheme",
			Pattern: regexp.MustCompile(`(?i)^(?:mailto|tel|javascript|data|ftp|file|ssh|ldap):`),
		},
		{
			// Static assets: common file extensions at the end of the path,
			// query string allowed (e.g. /app.js?v=1).
			Name:    "static-asset",
			Pattern: regexp.MustCompile(`(?i)\.(?:css|js|mjs|png|jpe?g|gif|svg|ico|webp|bmp|tif|tiff|woff2?|ttf|eot|mp[34]|m4a|avi|mov|webm|ogg|pdf|zip|gz|tar|rar|7z|dmg|iso|exe|dll|bin|wasm|map)(?:[?#].*)?$`),
		},
		{
			// Obviously meaningless navigation targets.
			Name:    "junk-path",
			Pattern: regexp.MustCompile(`(?i)/(?:javascript:|void\(0\)|about:blank)(?:[?#].*)?$`),
		},
	}
}
