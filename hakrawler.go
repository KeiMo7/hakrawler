package main

import (
	"bufio"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
)

type Result struct {
	Source string
	URL    string
	Where  string
}

var headers map[string]string

// Thread safe map
var sm sync.Map

func main() {
	inside := flag.Bool("i", false, "Only crawl inside path")
	threads := flag.Int("t", 8, "Number of threads to utilise.")
	depth := flag.Int("d", 2, "Depth to crawl.")
	maxSize := flag.Int("size", -1, "Page size limit, in KB.")
	insecure := flag.Bool("insecure", false, "Disable TLS verification.")
	subsInScope := flag.Bool("subs", false, "Include subdomains for crawling.")
	showJson := flag.Bool("json", false, "Output as JSON.")
	showSource := flag.Bool("s", false, "Show the source of URL based on where it was found. E.g. href, form, script, etc.")
	showWhere := flag.Bool("w", false, "Show at which link the URL is found.")
	rawHeaders := flag.String(("h"), "", "Custom headers separated by two semi-colons. E.g. -h \"Cookie: foo=bar;;Referer: http://example.com/\" ")
	unique := flag.Bool(("u"), false, "Show only unique urls.")
	proxy := flag.String(("proxy"), "", "Proxy URL. E.g. -proxy http://127.0.0.1:8080")
	timeout := flag.Int("timeout", -1, "Maximum time to crawl each URL from stdin, in seconds.")
	disableRedirects := flag.Bool("dr", false, "Disable following HTTP redirects.")

	// Resumable-crawl flags. Off by default; when -resume is absent the
	// program behaves exactly as before (same output, same exit codes).
	resumeDir := flag.String("resume", "", "Enable resumable crawling and store progress in this state directory. Re-run with the same directory to continue.")
	resumeSync := flag.String("resume-sync", syncModeGroup, "Durability mode for the journal: 'group' batches fsyncs (fast, a crash redoes at most one sync window), 'strict' fsyncs every record (slowest, zero record loss on SIGKILL).")
	resumeWindow := flag.Duration("resume-sync-window", defaultSyncWindow, "Max batching delay for -resume-sync=group before records are forced to disk.")
	snapshotPath := flag.String("resume-snapshot", "", "Path for an exportable JSON snapshot of the queue/progress (defaults to <resume-dir>/queue-snapshot.json).")
	snapshotInterval := flag.Duration("resume-snapshot-interval", 5*time.Second, "How often the queue snapshot is refreshed while resuming (0 disables periodic snapshots).")

	flag.Parse()

	if *proxy != "" {
		os.Setenv("PROXY", *proxy)
	}
	proxyURL, _ := url.Parse(os.Getenv("PROXY"))

	// Convert the headers input to a usable map (or die trying)
	err := parseHeaders(*rawHeaders)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error parsing headers:", err)
		os.Exit(1)
	}

	// Check for stdin input
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) != 0 {
		fmt.Fprintln(os.Stderr, "No urls detected. Hint: cat urls.txt | hakrawler")
		os.Exit(1)
	}

	// Read all seeds up front. The single-shot path still consumes stdin line
	// by line below; the resume path needs the full list to reconcile it with
	// the stored seed set before any crawling starts.
	seeds, readErr := readStdinSeeds()
	if readErr != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", readErr)
		os.Exit(1)
	}

	if *resumeDir != "" {
		runResumeCLI(seeds, crawlConfig{
			inside: *inside, threads: *threads, depth: *depth, maxSize: *maxSize,
			insecure: *insecure, subsInScope: *subsInScope, showJSON: *showJson,
			showSource: *showSource, showWhere: *showWhere, proxy: *proxy,
			timeout: *timeout, disableRedirects: *disableRedirects,
		}, *resumeDir, *resumeSync, *resumeWindow, *snapshotPath, *snapshotInterval, *unique)
		return
	}

	results := make(chan string, *threads)
	go func() {
		// get each line of stdin, push it to the work channel
		for _, url := range seeds {
			hostname, err := extractHostname(url)
			if err != nil {
				log.Println("Error parsing URL:", err)
				continue
			}

			allowed_domains := []string{hostname}
			// if "Host" header is set, append it to allowed domains
			if headers != nil {
				if val, ok := headers["Host"]; ok {
					allowed_domains = append(allowed_domains, val)
				}
			}

			// Instantiate default collector
			c := colly.NewCollector(
				// default user agent header
				colly.UserAgent("Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"),
				// set custom headers
				colly.Headers(headers),
				// limit crawling to the domain of the specified URL
				colly.AllowedDomains(allowed_domains...),
				// set MaxDepth to the specified depth
				colly.MaxDepth(*depth),
				// specify Async for threading
				colly.Async(true),
			)

			// set a page size limit
			if *maxSize != -1 {
				c.MaxBodySize = *maxSize * 1024
			}

			// if -subs is present, use regex to filter out subdomains in scope.
			if *subsInScope {
				c.AllowedDomains = nil
				c.URLFilters = []*regexp.Regexp{regexp.MustCompile(".*(\\.|\\/\\/)" + strings.ReplaceAll(hostname, ".", "\\.") + "((#|\\/|\\?).*)?")}
			}

			// If `-dr` flag provided, do not follow HTTP redirects.
			if *disableRedirects {
				c.SetRedirectHandler(func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				})
			}
			// Set parallelism
			c.Limit(&colly.LimitRule{DomainGlob: "*", Parallelism: *threads})

			// Print every href found, and visit it
			c.OnHTML("a[href]", func(e *colly.HTMLElement) {
				link := e.Attr("href")
				abs_link := e.Request.AbsoluteURL(link)
				if strings.Contains(abs_link, url) || !*inside {

					printResult(link, "href", *showSource, *showWhere, *showJson, results, e)
					e.Request.Visit(link)
				}
			})

			// find and print all the JavaScript files
			c.OnHTML("script[src]", func(e *colly.HTMLElement) {
				printResult(e.Attr("src"), "script", *showSource, *showWhere, *showJson, results, e)
			})

			// find and print all the form action URLs
			c.OnHTML("form[action]", func(e *colly.HTMLElement) {
				printResult(e.Attr("action"), "form", *showSource, *showWhere, *showJson, results, e)
			})

			// add the custom headers
			if headers != nil {
				c.OnRequest(func(r *colly.Request) {
					for header, value := range headers {
						r.Headers.Set(header, value)
					}
				})
			}

			if *proxy != "" {
				// Skip TLS verification for proxy, if -insecure specified
				c.WithTransport(&http.Transport{
					Proxy:           http.ProxyURL(proxyURL),
					TLSClientConfig: &tls.Config{InsecureSkipVerify: *insecure},
				})
			} else {
				// Skip TLS verification if -insecure flag is present
				c.WithTransport(&http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: *insecure},
				})
			}

			if *timeout == -1 {
				// Start scraping
				c.Visit(url)
				// Wait until threads are finished
				c.Wait()
			} else {
				finished := make(chan int, 1)

				go func() {
					// Start scraping
					c.Visit(url)
					// Wait until threads are finished
					c.Wait()
					finished <- 0
				}()

				select {
				case _ = <-finished: // the crawling finished before the timeout
					close(finished)
					continue
				case <-time.After(time.Duration(*timeout) * time.Second): // timeout reached
					log.Println("[timeout] " + url)
					continue

				}
			}

		}
		close(results)
	}()

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	urlsFound := false
	if *unique {
		for res := range results {
			if isUnique(res) {
				fmt.Fprintln(w, res)
				urlsFound = true
			}
		}
	}
	for res := range results {
		fmt.Fprintln(w, res)
		urlsFound = true
	}

	if !urlsFound {
		fmt.Fprintln(os.Stderr, "No URLs were found. This usually happens when a domain is specified (https://example.com), but it redirects to a subdomain (https://www.example.com). The subdomain is not included in the scope, so the no URLs are printed. In order to overcome this, either specify the final URL in the redirect chain or use the -subs option to include subdomains.")
	}
}

// readStdinSeeds reads every non-empty stdin line as a seed URL, preserving
// order and duplicates as given.
func readStdinSeeds() ([]string, error) {
	var out []string
	s := bufio.NewScanner(os.Stdin)
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line != "" {
			out = append(out, line)
		}
	}
	return out, s.Err()
}

// runResumeCLI is the entry point of the resume-enabled path. All failure
// modes exit non-zero with an explicit message and, where applicable, a
// recovery instruction.
func runResumeCLI(seeds []string, cfg crawlConfig, dir, syncMode string, syncWindow time.Duration, snapPath string, snapInterval time.Duration, unique bool) {
	j, err := openJournal(dir, syncMode, syncWindow)
	if err != nil {
		fmt.Fprintln(os.Stderr, "resume:", err)
		os.Exit(1)
	}
	defer j.Close()

	added, err := j.InitSeeds(seeds)
	if err != nil {
		fmt.Fprintln(os.Stderr, "resume:", err)
		os.Exit(1)
	}
	if len(added) > 0 {
		fmt.Fprintf(os.Stderr, "resume: %d new seed(s) merged into the queue\n", len(added))
	}
	// Fold accumulated history back to one record per URL/output so long,
	// repeatedly resumed crawls do not grow the journal without bound.
	if err := j.Compact(); err != nil {
		fmt.Fprintln(os.Stderr, "resume: journal compaction failed:", err)
		os.Exit(1)
	}

	if snapPath == "" {
		snapPath = filepath.Join(dir, "queue-snapshot.json")
	}

	// Periodic, throttled snapshot export so an operator can see which
	// address the crawl is stuck on. Writes are atomic (tmp+rename).
	stopSnap := make(chan struct{})
	if snapInterval > 0 {
		go func() {
			t := time.NewTicker(snapInterval)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					if err := j.ExportSnapshot(snapPath); err != nil {
						fmt.Fprintln(os.Stderr, "resume: snapshot export failed:", err)
					}
				case <-stopSnap:
					return
				}
			}
		}()
	}

	results := make(chan string, cfg.threads)
	sink := channelSink{ch: results}

	var runErr error
	go func() {
		runErr = runResume(cfg, j, seeds, unique, sink, func(Snapshot) {})
		close(results)
	}()

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	urlsFound := false

	// Output handling mirrors the single-shot path. With -u the same in-memory
	// dedup is applied; without -u the journal already stores each result
	// identity once per run chain, matching one-shot semantics for repeated
	// discoveries within a crawl.
	if unique {
		for res := range results {
			if isUnique(res) {
				fmt.Fprintln(w, res)
				urlsFound = true
			}
		}
	} else {
		for res := range results {
			fmt.Fprintln(w, res)
			urlsFound = true
		}
	}
	close(stopSnap)

	// Always leave a final, fresh snapshot behind (even on failure) so the
	// queue position at exit is inspectable.
	if err := j.ExportSnapshot(snapPath); err != nil {
		fmt.Fprintln(os.Stderr, "resume: final snapshot export failed:", err)
	}

	if runErr != nil {
		fmt.Fprintln(os.Stderr, "resume:", runErr)
		os.Exit(1)
	}

	if !urlsFound {
		fmt.Fprintln(os.Stderr, "No URLs were found. This usually happens when a domain is specified (https://example.com), but it redirects to a subdomain (https://www.example.com). The subdomain is not included in the scope, so the no URLs are printed. In order to overcome this, either specify the final URL in the redirect chain or use the -subs option to include subdomains.")
	}
}

// parseHeaders does validation of headers input and saves it to a formatted map.
func parseHeaders(rawHeaders string) error {
	if rawHeaders != "" {
		if !strings.Contains(rawHeaders, ":") {
			return errors.New("headers flag not formatted properly (no colon to separate header and value)")
		}

		headers = make(map[string]string)
		rawHeaders := strings.Split(rawHeaders, ";;")
		for _, header := range rawHeaders {
			var parts []string
			if strings.Contains(header, ": ") {
				parts = strings.SplitN(header, ": ", 2)
			} else if strings.Contains(header, ":") {
				parts = strings.SplitN(header, ":", 2)
			} else {
				continue
			}
			headers[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return nil
}

// extractHostname() extracts the hostname from a URL and returns it
func extractHostname(urlString string) (string, error) {
	u, err := url.Parse(urlString)
	if err != nil || !u.IsAbs() {
		return "", errors.New("Input must be a valid absolute URL")
	}

	return u.Hostname(), nil
}

// print result constructs output lines and sends them to the results chan
func printResult(link string, sourceName string, showSource bool, showWhere bool, showJson bool, results chan string, e *colly.HTMLElement) {
	result := e.Request.AbsoluteURL(link)
	whereURL := e.Request.URL.String()
	if result != "" {
		if showJson {
			where := ""
			if showWhere {
				where = whereURL
			}
			bytes, _ := json.Marshal(Result{
				Source: sourceName,
				URL:    result,
				Where:  where,
			})
			result = string(bytes)
		} else if showSource {
			result = "[" + sourceName + "] " + result
		}

		if showWhere && !showJson {
			result = "[" + whereURL + "] " + result
		}

		// If timeout occurs before goroutines are finished, recover from panic that may occur when attempting writing to results to closed results channel
		defer func() {
			if err := recover(); err != nil {
				return
			}
		}()
		results <- result
	}
}

// returns whether the supplied url is unique or not
func isUnique(url string) bool {
	_, present := sm.Load(url)
	if present {
		return false
	}
	sm.Store(url, true)
	return true
}
