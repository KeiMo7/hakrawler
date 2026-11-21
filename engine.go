package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocolly/colly/v2"
)

// BoundaryConfig carries every flag the boundary-aware crawl path needs.
// Legacy flags keep their original meaning; only -boundary switches the
// program onto this code path.
type BoundaryConfig struct {
	Threads         int
	Depth           int
	MaxSize         int
	Insecure        bool
	ShowJSON        bool
	ShowSource      bool
	ShowWhere       bool
	Unique          bool
	Proxy           string
	Timeout         int
	DisableRedirect bool
	Headers         map[string]string
	Scope           ScopeMode
	Whitelist       []string
	Inside          bool
	Rules           []ExclusionRule
	StatsFile       string
}

// discovery is one link extracted from a page, in extraction order.
type discovery struct {
	seq    int
	source string // href | script | form
	raw    string // attribute value as written in the page
	abs    string // resolved absolute URL (colly's AbsoluteURL)
	where  string // page URL it was found on
}

// emittedLine is one buffered output record; sorting these before each flush
// is what makes output order independent of goroutine scheduling.
type emittedLine struct {
	key  discovery
	line string
}

// RunBoundaryCrawl is the entry point of the boundary-aware path. It reads
// every seed up front (seed validation is a startup-phase decision), then
// processes seeds in input order. The returned error is reserved for
// output-write failures; all per-page failures are counted and crawled past.
func RunBoundaryCrawl(cfg BoundaryConfig, seeds []string, out *bufio.Writer) (*Stats, error) {
	stats := &Stats{RuleHits: map[string]int64{}}
	guard := NewGuard(cfg.Depth, cfg.Scope, "", cfg.Whitelist, cfg.Rules, stats)

	// ---- Startup phase: validate every seed before any request (三.2) ----
	type acceptedSeed struct {
		raw        string
		canonical  string
		insidePath string
	}
	var accepted []acceptedSeed
	for _, seed := range seeds {
		seed = strings.TrimSpace(seed)
		if seed == "" {
			continue
		}
		stats.Seeds.Add(1)
		canonical, ok := NormalizeURL(seed)
		if !ok {
			stats.RecordSeedRejection(seed, "seed is not a valid absolute URL")
			continue
		}
		insidePath := ""
		if cfg.Inside {
			if u, err := url.Parse(canonical); err == nil {
				insidePath = u.Path
				if insidePath == "" {
					insidePath = "/"
				}
			}
		}
		guard.insidePath = insidePath
		if okScope, reason := guard.AddSeed(canonical); !okScope {
			stats.RecordSeedRejection(seed, reason)
			continue
		}
		guard.insidePath = ""
		accepted = append(accepted, acceptedSeed{raw: seed, canonical: canonical, insidePath: insidePath})
	}

	if stats.SeedsRejected.Load() > 0 {
		for _, r := range stats.PerSeed {
			fmt.Fprintf(os.Stderr, "seed rejected: %s (%s)\n", r.Seed, r.Reason)
		}
	}
	if len(accepted) == 0 {
		return stats, nil
	}

	urlsFound := false

	for _, seed := range accepted {
		// Reset the path-prefix scope for this seed; host scope is additive
		// and shared across seeds, matching stdin's multi-seed behaviour.
		guard.insidePath = seed.insidePath
		guard.MarkSeeded(seed.canonical)

		timedOut, err := crawlSeed(cfg, guard, stats, seed.raw, seed.canonical, out, &urlsFound)
		if err != nil {
			return stats, err
		}
		if timedOut {
			stats.TimedOut.Add(1)
		}
	}

	if !urlsFound {
		fmt.Fprintln(os.Stderr, "No URLs were found. This usually happens when a domain is specified (https://example.com), but it redirects to a subdomain (https://www.example.com). The subdomain is not included in the scope, so the no URLs are printed. In order to overcome this, either specify the final URL in the redirect chain or use the -scope domain option.")
	}
	return stats, nil
}

// crawlSeed runs a depth-bounded BFS for one seed. Depth levels are processed
// strictly in order: every page of level N is fetched (concurrently) and all
// its output flushed before level N+1 starts, which is what gives stable
// ordering while still streaming results to disk.
func crawlSeed(cfg BoundaryConfig, guard *Guard, stats *Stats, seedRaw, seedCanonical string, out *bufio.Writer, urlsFound *bool) (bool, error) {
	c := newBoundaryCollector(cfg)

	// Discoveries are appended directly under levelMu: the level barrier
	// (levelWG) returns only after every fetch callback has completed, so by
	// the time the BFS driver reads `current` every append is already
	// visible. A separate drain goroutine would race the barrier — its
	// channel receives are not covered by OnScraped — so it is deliberately
	// avoided here.
	var levelMu sync.Mutex
	var current []discovery
	// seq is shared by all three extractors so each discovery carries one
	// global in-page order number instead of three per-selector counters
	// (colly's HTMLElement.Index restarts at 0 for every selector).
	var seq atomic.Uint64

	registerCallbacks(c, &levelMu, &current, &seq)
	// Level barrier: colly's internal WaitGroup is not reusable once it has
	// reached zero (a second Wait() on a zeroed WaitGroup blocks forever), so
	// the BFS levels cannot synchronise on c.Wait(). OnScraped/OnError fire
	// exactly once per scheduled request and together cover every terminal
	// state, which gives us a per-level barrier that is safe to reset.
	var levelWG sync.WaitGroup
	c.OnScraped(func(*colly.Response) { levelWG.Done() })
	c.OnError(func(r *colly.Response, err error) {
		levelWG.Done()
		// colly routes three distinct failures here:
		//  1. transport failure (peer reset, truncated body, timeout) ->
		//     Response is nil or has status 0;
		//  2. HTTP error status (>= 203 in colly's default) -> status set;
		//  3. HTML parse failure after a received body -> status set and the
		//     error is not an HTTP status text.
		// Only case 3 increments ParseErrors; case 1 increments FetchErrors;
		// case 2 is logged but not an error of the crawl pipeline itself.
		switch {
		case r == nil || r.StatusCode == 0:
			stats.FetchErrors.Add(1)
			fmt.Fprintf(os.Stderr, "fetch error: %v\n", err)
		case isHTMLErrorStatus(r.StatusCode):
			fmt.Fprintf(os.Stderr, "http error: %s: %s\n", r.Request.URL, err)
		default:
			stats.ParseErrors.Add(1)
			fmt.Fprintf(os.Stderr, "parse error: %s: %v\n", r.Request.URL, err)
		}
	})

	// Per-seed timeout is a context deadline: in-flight requests get
	// cancelled, the level finishes with whatever returned, and the seed is
	// marked timed out. No goroutine outlives the function.
	ctx, cancel := context.WithCancel(context.Background())
	if cfg.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(cfg.Timeout)*time.Second)
	}
	defer cancel()
	c.Context = ctx

	timedOut := false
	frontier := []string{seedCanonical}

	for depth := 1; len(frontier) > 0 && depth <= cfg.Depth; depth++ {
		if err := ctx.Err(); err != nil {
			break
		}
		levelMu.Lock()
		current = current[:0]
		levelMu.Unlock()
		seq.Store(0)

		// Feed the level into colly. With Async(true), Visit only schedules
		// work (the goroutine is spawned inside scrape). The WaitGroup slot
		// MUST be reserved before Visit: otherwise the fetch goroutine can
		// reach OnScraped/Done before the Add, leaving the counter negative
		// or the barrier passing early. A non-nil Visit error means no
		// goroutine was spawned, so the reserved slot is released again.
		for _, u := range frontier {
			if err := ctx.Err(); err != nil {
				break
			}
			levelWG.Add(1)
			if err := c.Visit(u); err != nil {
				levelWG.Done()
				stats.FetchErrors.Add(1)
				fmt.Fprintf(os.Stderr, "schedule error: %s: %v\n", u, err)
			}
		}
		levelWG.Wait()

		if ctx.Err() == context.DeadlineExceeded {
			timedOut = true
			fmt.Fprintf(os.Stderr, "[timeout] %s\n", seedRaw)
		}

		levelMu.Lock()
		items := append([]discovery(nil), current...)
		levelMu.Unlock()
		// Deterministic order independent of goroutine scheduling: page URL,
		// then in-document element index, then raw attribute value.
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].where != items[j].where {
				return items[i].where < items[j].where
			}
			if items[i].seq != items[j].seq {
				return items[i].seq < items[j].seq
			}
			return items[i].raw < items[j].raw
		})

		var nextFrontier []string
		var lines []emittedLine
		seenNext := map[string]bool{}
		printed := map[string]bool{}

		for _, d := range items {
			// Emit the link in the legacy output formats. Empty resolved
			// URLs (e.g. href="") are dropped exactly as legacy does.
			if d.abs != "" {
				key := d.abs
				if canon, ok := NormalizeURL(d.abs); ok {
					key = canon
				}
				// -u reuses the same NormalizeURL definition: output dedup
				// and crawl dedup can never disagree about identity.
				if !(cfg.Unique && printed[key]) {
					printed[key] = true
					lines = append(lines, emittedLine{key: d, line: formatResult(cfg, d)})
					*urlsFound = true
				}
			}

			if d.abs == "" {
				continue
			}

			// Every extracted link goes through the same pre-enqueue
			// pipeline so the accounting invariant holds for the whole
			// candidate set; the would-be depth is one beyond the page it
			// was found on. No goroutine or HTTP request is ever created for
			// a link the guard refuses.
			decision, canonical, _ := guard.Classify(d.abs, depth+1)

			// Only anchors drive the crawl. A script/form URL that passes
			// the guard is still counted as enqueued (it would have been
			// fetchable) but never scheduled, matching the legacy behaviour
			// where only a[href] calls Visit.
			if d.source != "href" {
				continue
			}
			if decision == DecisionEnqueue && !seenNext[canonical] {
				seenNext[canonical] = true
				nextFrontier = append(nextFrontier, canonical)
			}
		}

		sort.SliceStable(lines, func(i, j int) bool {
			a, b := lines[i].key, lines[j].key
			if a.where != b.where {
				return a.where < b.where
			}
			if a.seq != b.seq {
				return a.seq < b.seq
			}
			if a.source != b.source {
				return a.source < b.source
			}
			return a.raw < b.raw
		})
		for _, l := range lines {
			if _, err := fmt.Fprintln(out, l.line); err != nil {
				return timedOut, err
			}
		}
		if err := out.Flush(); err != nil {
			return timedOut, err
		}

		frontier = nextFrontier
	}

	cancel()
	return timedOut, nil
}

// isHTMLErrorStatus reports whether a colly OnError status code is an HTTP
// error response rather than a parse/transport failure. colly's default
// ParseHTTPErrorResponse is false, so only statuses >= 203 surface as errors;
// the 4xx/5xx bands are what an operator means by an "HTTP error".
func isHTMLErrorStatus(status int) bool {
	return status >= 400
}

// newBoundaryCollector builds a colly collector whose transport-level
// configuration mirrors the legacy path. Domain/depth enforcement lives in
// the Guard, so the collector itself is deliberately unrestricted: this keeps
// "check before enqueue" in one place instead of splitting it across colly's
// requestCheck and our code.
func newBoundaryCollector(cfg BoundaryConfig) *colly.Collector {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"),
		colly.Headers(cfg.Headers),
		colly.Async(true),
		// AllowURLRevisit is on because dedup is owned by the Guard's
		// canonical-URL set; colly's raw-string revisit check would otherwise
		// reject links that the guard intentionally admitted.
		colly.AllowURLRevisit(),
	)
	if cfg.MaxSize != -1 {
		c.MaxBodySize = cfg.MaxSize * 1024
	}
	if cfg.DisableRedirect {
		c.SetRedirectHandler(func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		})
	}
	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: cfg.Threads})

	proxyURL, _ := url.Parse(os.Getenv("PROXY"))
	if cfg.Proxy != "" {
		c.WithTransport(&http.Transport{
			Proxy:           http.ProxyURL(proxyURL),
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.Insecure},
		})
	} else {
		c.WithTransport(&http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.Insecure},
		})
	}
	if cfg.Headers != nil {
		c.OnRequest(func(r *colly.Request) {
			for header, value := range cfg.Headers {
				r.Headers.Set(header, value)
			}
		})
	}
	return c
}

// registerCallbacks wires the same three extractors the legacy crawler uses
// (a[href], script[src], form[action]) into the discovery channel. No
// Visit() call happens here — enqueueing is the BFS driver's exclusive job,
// which is what makes the pre-enqueue guarantee structural rather than
// conventional.
func registerCallbacks(c *colly.Collector, levelMu *sync.Mutex, current *[]discovery, seq *atomic.Uint64) {
	extractor := func(selector, attr, source string) {
		c.OnHTML(selector, func(e *colly.HTMLElement) {
			raw := e.Attr(attr)
			abs := e.Request.AbsoluteURL(raw)
			d := discovery{
				seq:    int(seq.Add(1) - 1),
				source: source,
				raw:    raw,
				abs:    abs,
				where:  e.Request.URL.String(),
			}
			levelMu.Lock()
			*current = append(*current, d)
			levelMu.Unlock()
		})
	}
	extractor("a[href]", "href", "href")
	extractor("script[src]", "src", "script")
	extractor("form[action]", "action", "form")
}

// formatResult renders a discovered link using exactly the legacy output
// shapes, so the only output difference in boundary mode is ordering.
func formatResult(cfg BoundaryConfig, d discovery) string {
	result := d.abs
	if cfg.ShowJSON {
		where := ""
		if cfg.ShowWhere {
			where = d.where
		}
		bytes, _ := json.Marshal(Result{Source: d.source, URL: result, Where: where})
		return string(bytes)
	}
	if cfg.ShowSource {
		result = "[" + d.source + "] " + result
	}
	if cfg.ShowWhere {
		result = "[" + d.where + "] " + result
	}
	return result
}

// runBoundaryMode wires parsed flags into the boundary-aware crawl path and
// returns the process exit code. Exit-code semantics are unchanged from the
// legacy binary for normal runs (0); exit 2 means every seed was refused at
// startup and exit 1 is reserved for configuration/output failures.
func runBoundaryMode(scopeName, whitelistFlag, rulesFlag, statsFile string,
	inside bool, threads, depth, maxSize int, insecure, showJSON, showSource,
	showWhere, unique bool, proxy string, timeout int, disableRedirect bool) int {

	mode := ScopeMode(scopeName)
	if mode != ScopeHost && mode != ScopeDomain && mode != ScopeWhitelist {
		fmt.Fprintf(os.Stderr, "invalid -scope %q: want host, domain or whitelist\n", scopeName)
		return 1
	}
	var whitelist []string
	if mode == ScopeWhitelist {
		for _, h := range strings.Split(whitelistFlag, ",") {
			if h = strings.TrimSpace(h); h != "" {
				whitelist = append(whitelist, h)
			}
		}
		if len(whitelist) == 0 {
			fmt.Fprintln(os.Stderr, "-scope=whitelist requires at least one host in -whitelist")
			return 1
		}
	} else if whitelistFlag != "" {
		fmt.Fprintln(os.Stderr, "-whitelist is only used together with -scope=whitelist")
		return 1
	}

	var userRules []ExclusionRule
	if rulesFlag != "" {
		var err error
		userRules, err = ParseUserRules(splitCSV(rulesFlag))
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid -rules entry:", err)
			return 1
		}
	}
	// User rules first so their names win in hit accounting; built-ins still
	// cover anything the user did not name explicitly.
	rules := append(append([]ExclusionRule{}, userRules...), BuiltinRules()...)

	var seeds []string
	scanner := bufio.NewScanner(os.Stdin)
	// Large sites may carry long URLs; raise the per-line ceiling above the
	// Scanner default of 64 KiB.
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			seeds = append(seeds, line)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
		return 1
	}
	if len(seeds) == 0 {
		fmt.Fprintln(os.Stderr, "No urls detected. Hint: cat urls.txt | hakrawler")
		return 1
	}

	cfg := BoundaryConfig{
		Threads:         threads,
		Depth:           depth,
		MaxSize:         maxSize,
		Insecure:        insecure,
		ShowJSON:        showJSON,
		ShowSource:      showSource,
		ShowWhere:       showWhere,
		Unique:          unique,
		Proxy:           proxy,
		Timeout:         timeout,
		DisableRedirect: disableRedirect,
		Headers:         headers,
		Scope:           mode,
		Whitelist:       whitelist,
		Inside:          inside,
		Rules:           rules,
		StatsFile:       statsFile,
	}

	out := bufio.NewWriter(os.Stdout)
	stats, err := RunBoundaryCrawl(cfg, seeds, out)
	// Flush whatever was produced before deciding the exit code; an output
	// failure itself is fatal (三.3) and must not be masked.
	if ferr := out.Flush(); ferr != nil {
		fmt.Fprintln(os.Stderr, "writing output:", ferr)
		return 1
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "crawl aborted:", err)
		return 1
	}

	snap := stats.Snapshot()
	if err := WriteStats(snap, statsFile); err != nil {
		fmt.Fprintln(os.Stderr, "writing stats:", err)
		return 1
	}
	if snap.Seeds > 0 && snap.SeedsRejected == snap.Seeds {
		// All seeds refused at startup: distinct non-zero code so callers can
		// tell configuration errors apart from a successful empty crawl.
		return 2
	}
	return 0
}

// splitCSV splits a comma-separated flag value while tolerating spaces.
func splitCSV(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// ParseUserRules compiles -rules entries of the form "name=regex". A bare
// regex without a name gets an auto-generated name so hit accounting still
// has a key.
func ParseUserRules(entries []string) ([]ExclusionRule, error) {
	var rules []ExclusionRule
	for i, entry := range entries {
		name, pattern := entry, entry
		if idx := strings.Index(entry, "="); idx >= 0 {
			name, pattern = entry[:idx], entry[idx+1:]
		} else {
			name = fmt.Sprintf("user-rule-%d", i+1)
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", name, err)
		}
		rules = append(rules, ExclusionRule{Name: name, Pattern: re})
	}
	return rules, nil
}
