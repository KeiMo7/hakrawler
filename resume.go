package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
)

// crawlConfig carries every flag that influences crawling. The single-shot
// path and the resume path build their collectors from the same struct, so
// the two paths cannot drift in scope/depth/header/transport behaviour.
type crawlConfig struct {
	inside           bool
	threads          int
	depth            int
	maxSize          int
	insecure         bool
	subsInScope      bool
	showJSON         bool
	showSource       bool
	showWhere        bool
	proxy            string
	timeout          int
	disableRedirects bool
}

// outputSink receives the exact lines that the single-shot path would print.
// The single-shot path wires it to the results channel; the resume path
// wires it to the journal so output survives a restart.
type outputSink interface {
	Send(line string, resultURL string)
}

// channelSink is the single-shot sink (preserves the original behaviour).
type channelSink struct{ ch chan string }

func (s channelSink) Send(line, _ string) {
	defer func() { _ = recover() }() // writing to a closed channel on timeout
	s.ch <- line
}

func jsonMarshal(r Result) ([]byte, error) { return json.Marshal(r) }

func stderrWriter() *os.File { return os.Stderr }

// buildCollector is the single shared constructor used by both paths. It is
// a faithful extraction of the collector setup that previously lived inline
// in main(); no crawling default changes here.
func buildCollector(rawSeedURL string, cfg crawlConfig) (*colly.Collector, error) {
	hostname, err := extractHostname(rawSeedURL)
	if err != nil {
		return nil, err
	}

	allowedDomains := []string{hostname}
	if headers != nil {
		if val, ok := headers["Host"]; ok {
			allowedDomains = append(allowedDomains, val)
		}
	}

	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"),
		colly.Headers(headers),
		colly.AllowedDomains(allowedDomains...),
		colly.MaxDepth(cfg.depth),
		colly.Async(true),
	)

	if cfg.maxSize != -1 {
		c.MaxBodySize = cfg.maxSize * 1024
	}

	if cfg.subsInScope {
		c.AllowedDomains = nil
		c.URLFilters = []*regexp.Regexp{regexp.MustCompile(".*(\\.|\\/\\/)" + strings.ReplaceAll(hostname, ".", "\\.") + "((#|\\/|\\?).*)?")}
	}

	if cfg.disableRedirects {
		c.SetRedirectHandler(func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		})
	}

	c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: cfg.threads})

	if cfg.proxy != "" {
		proxyURL, _ := url.Parse(cfg.proxy)
		c.WithTransport(&http.Transport{
			Proxy:           http.ProxyURL(proxyURL),
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.insecure},
		})
	} else {
		c.WithTransport(&http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.insecure},
		})
	}

	if headers != nil {
		c.OnRequest(func(r *colly.Request) {
			for header, value := range headers {
				r.Headers.Set(header, value)
			}
		})
	}
	return c, nil
}

// attachEmitOnlyCallbacks reproduces the three printResult callbacks but
// never triggers visits. The resume path uses it and performs visits itself
// so it can persist identities and enforce stored depths. seedURL is the
// exact seed string used by the single-shot inside-path gate
// (strings.Contains(absLink, seedURL)).
func attachEmitOnlyCallbacks(c *colly.Collector, cfg crawlConfig, seedURL string, sink outputSink) {
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		absLink := e.Request.AbsoluteURL(link)
		if strings.Contains(absLink, seedURL) || !cfg.inside {
			emitResult(link, "href", cfg, sink, e)
		}
	})
	c.OnHTML("script[src]", func(e *colly.HTMLElement) {
		emitResult(e.Attr("src"), "script", cfg, sink, e)
	})
	c.OnHTML("form[action]", func(e *colly.HTMLElement) {
		emitResult(e.Attr("action"), "form", cfg, sink, e)
	})
}

// emitResult mirrors printResult exactly, but routes the finished line to a
// sink instead of the results channel.
func emitResult(link, sourceName string, cfg crawlConfig, sink outputSink, e *colly.HTMLElement) {
	result := e.Request.AbsoluteURL(link)
	whereURL := e.Request.URL.String()
	if result == "" {
		return
	}
	resultURL := result
	if cfg.showJSON {
		where := ""
		if cfg.showWhere {
			where = whereURL
		}
		bytes, _ := jsonMarshal(Result{Source: sourceName, URL: result, Where: where})
		result = string(bytes)
	} else if cfg.showSource {
		result = "[" + sourceName + "] " + result
	}
	if cfg.showWhere && !cfg.showJSON {
		result = "[" + whereURL + "] " + result
	}
	sink.Send(result, resultURL)
}

// classifyFailure maps a colly error to one of the traceable categories
// stored in the journal. Deterministic by construction: the same failure
// always yields the same category.
func classifyFailure(r *colly.Response, err error) (string, string) {
	if r != nil && r.StatusCode != 0 {
		// A response arrived with a status colly treats as an error (>=500).
		return failHTTP, fmt.Sprintf("HTTP %d", r.StatusCode)
	}
	if err != nil && isMalformed(err) {
		return failMalformed, truncate(err.Error(), 512)
	}
	return failTransport, truncate(errString(err), 512)
}

func isMalformed(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// charset/body parse failures are the "upstream returned malformed data" class
	return strings.Contains(msg, "charset") || strings.Contains(msg, "invalid") ||
		strings.Contains(msg, "parse") || strings.Contains(msg, "unexpected")
}

func errString(err error) string {
	if err == nil {
		return "unknown error"
	}
	return err.Error()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// journalSink records every emitted line into the journal before it reaches
// stdout, so a kill cannot lose an output that was already discovered.
// unique mirrors the single-shot -u flag across run boundaries: within one
// process the channel-stage isUnique() still applies, and unique additionally
// suppresses lines whose identity was already printed in an earlier run.
type journalSink struct {
	j      *Journal
	ch     chan string
	unique bool
	seen   map[string]bool
}

func (s *journalSink) Send(line, resultURL string) {
	if resultURL != "" {
		norm, err := normalizeURL(resultURL)
		if err != nil {
			norm = resultURL
		}
		if s.unique {
			if s.seen[norm] {
				return
			}
			s.seen[norm] = true
		}
		if _, err := s.j.RecordOutput(resultURL, line); err != nil {
			fmt.Fprintln(os.Stderr, "journal output write failed:", err)
		}
	}
	defer func() { _ = recover() }()
	s.ch <- line
}

const (
	depthKey = "_hr_depth"
)

// runResume executes one resume-enabled crawl and blocks until every pending
// URL has been processed. Replaying prior outputs first keeps the final
// stdout identical in content to a one-shot run.
func runResume(cfg crawlConfig, j *Journal, rawSeeds []string, unique bool, out outputSink, onProgress func(Snapshot)) error {
	// 1. Replay outputs from previous run(s) in original discovery order.
	for _, line := range j.ReplayOutputs() {
		out.Send(line, "")
	}

	// 2. Enqueue brand-new seeds added to the seed list (already validated
	//    against removals in InitSeeds).
	for _, s := range rawSeeds {
		n, err := normalizeURL(s)
		if err != nil {
			continue
		}
		if _, err := j.MarkQueued(n, 1, n); err != nil {
			return err
		}
	}

	// 3. Crawl seed by seed, preserving the single-shot per-seed grouping.
	for _, seed := range j.Seeds() {
		if err := crawlOneSeedResume(cfg, j, seed, unique, out, onProgress); err != nil {
			return err
		}
	}
	return nil
}

// crawlOneSeedResume fetches all pending URLs that belong to one seed.
func crawlOneSeedResume(cfg crawlConfig, j *Journal, seed string, unique bool, out outputSink, onProgress func(Snapshot)) error {
	c, err := buildCollector(seed, cfg)
	if err != nil {
		return err
	}

	sink := &journalSink{j: j, ch: make(chan string, cfg.threads), unique: unique, seen: map[string]bool{}}
	var sinkWG sync.WaitGroup
	sinkWG.Add(1)
	go func() {
		defer sinkWG.Done()
		for line := range sink.ch {
			out.Send(line, "")
		}
	}()

	// Output-only callbacks (no visits): exact single-shot print semantics.
	attachEmitOnlyCallbacks(c, cfg, seed, sink)

	// Discovery + persistence callback: the only place child visits happen.
	c.OnHTML("a[href]", func(e *colly.HTMLElement) {
		link := e.Attr("href")
		abs := e.Request.AbsoluteURL(link)
		if abs == "" {
			return
		}
		// Mirror the single-shot inside-path visit gate exactly: the original
		// code tests strings.Contains(absLink, seedURL).
		if cfg.inside && !strings.Contains(abs, seed) {
			return
		}
		norm, err := normalizeURL(abs)
		if err != nil {
			return
		}
		parentDepth := 1
		if v := e.Request.Ctx.GetAny(depthKey); v != nil {
			parentDepth = v.(int)
		}
		childDepth := parentDepth + 1
		if cfg.depth > 0 && childDepth > cfg.depth {
			return
		}
		isNew, err := j.MarkQueued(norm, childDepth, seed)
		if err != nil {
			fmt.Fprintln(os.Stderr, "journal queue write failed:", err)
			return
		}
		if !isNew {
			// Known identity (completed/queued/failed in this or an earlier
			// run): colly's in-memory set would fetch it again, so suppress.
			return
		}
		childCtx := colly.NewContext()
		childCtx.Put(depthKey, childDepth)
		if verr := c.Request("GET", norm, nil, childCtx, nil); verr != nil {
			// Scope/depth refusals are normal control flow.
			if verr != colly.ErrForbiddenDomain && verr != colly.ErrNoURLFiltersMatch &&
				verr != colly.ErrForbiddenURL && verr != colly.ErrMaxDepth &&
				verr != colly.ErrAlreadyVisited {
				ft, detail := classifyFailure(nil, verr)
				_ = j.MarkFailed(norm, ft, detail)
			}
		}
	})

	// Failures: only journal-tracked URLs belonging to this seed are marked.
	c.OnError(func(r *colly.Response, err error) {
		var raw string
		if r != nil && r.Request != nil && r.Request.URL != nil {
			raw = r.Request.URL.String()
		}
		norm, nerr := normalizeURL(raw)
		if nerr != nil {
			return
		}
		st, ok := j.StateOf(norm)
		if !ok || st.Seed != seed {
			return
		}
		ft, detail := classifyFailure(r, err)
		if ferr := j.MarkFailed(norm, ft, detail); ferr != nil {
			fmt.Fprintln(os.Stderr, "journal failure write failed:", ferr)
		}
	})

	// Completion is marked after the full response (including link discovery)
	// has run. A kill between the last child journal write and this mark
	// leaves the parent "queued"; on resume the parent is refetched and every
	// child is already deduped — no link lost, no duplicate output.
	c.OnScraped(func(resp *colly.Response) {
		norm, err := normalizeURL(resp.Request.URL.String())
		if err != nil {
			return
		}
		st, ok := j.StateOf(norm)
		if !ok || st.Seed != seed {
			return
		}
		if cerr := j.MarkCompleted(norm); cerr != nil {
			fmt.Fprintln(os.Stderr, "journal completion write failed:", cerr)
		}
		if onProgress != nil {
			onProgress(j.Snapshot())
		}
	})

	// Seed the collector with every pending URL of this seed at its stored
	// depth. Pending includes previously failed URLs -> retry with trace.
	pending := j.Pending()
	for _, item := range pending {
		if item.Seed != seed {
			continue
		}
		ctx := colly.NewContext()
		ctx.Put(depthKey, item.Depth)
		if rerr := c.Request("GET", item.URL, nil, ctx, nil); rerr != nil {
			if rerr == colly.ErrAlreadyVisited {
				continue
			}
			ft, detail := classifyFailure(nil, rerr)
			_ = j.MarkFailed(item.URL, ft, detail)
		}
	}

	waitDone := make(chan struct{})
	go func() {
		c.Wait()
		close(waitDone)
	}()
	var timeoutErr error
	if cfg.timeout > 0 {
		select {
		case <-waitDone:
		case <-time.After(time.Duration(cfg.timeout) * time.Second):
			// Mirrors single-shot per-seed -timeout: in-flight URLs stay
			// queued/failed in the journal and are retried next run.
			timeoutErr = fmt.Errorf("timeout after %ds for seed %s", cfg.timeout, seed)
		}
	} else {
		<-waitDone
	}
	// Close the per-seed sink and wait for its forwarder to flush every line
	// before returning, so no output goroutine leaks across seeds and no line
	// is lost on the timeout path (the journal already holds it durably).
	close(sink.ch)
	sinkWG.Wait()
	return timeoutErr
}
