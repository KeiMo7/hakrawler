# Hakrawler

Fast golang web crawler for gathering URLs and JavaScript file locations. This is basically a simple implementation of the awesome Gocolly library.

## Example usages

Single URL:

```
echo https://google.com | hakrawler
```

Multiple URLs:

```
cat urls.txt | hakrawler
```

Timeout for each line of stdin after 5 seconds:

```
cat urls.txt | hakrawler -timeout 5
```

Send all requests through a proxy:

```
cat urls.txt | hakrawler -proxy http://localhost:8080
```

Include subdomains:

```
echo https://google.com | hakrawler -subs
```

> Note: a common issue is that the tool returns no URLs. This usually happens when a domain is specified (https://example.com), but it redirects to a subdomain (https://www.example.com). The subdomain is not included in the scope, so the no URLs are printed. In order to overcome this, either specify the final URL in the redirect chain or use the `-subs` option to include subdomains.

## Example tool chain

Get all subdomains of google, find the ones that respond to http(s), crawl them all.

```
echo google.com | haktrails subdomains | httpx | hakrawler
```

## Installation

### Normal Install

First, you'll need to [install go](https://golang.org/doc/install).

Then run this command to download + compile hakrawler:
```
go install github.com/hakluke/hakrawler@latest
```

You can now run `~/go/bin/hakrawler`. If you'd like to just run `hakrawler` without the full path, you'll need to `export PATH="~/go/bin/:$PATH"`. You can also add this line to your `~/.bashrc` file if you'd like this to persist.

### Docker Install (from dockerhub)

```
echo https://www.google.com | docker run --rm -i hakluke/hakrawler:v2 -subs
```

### Local Docker Install

It's much easier to use the dockerhub method above, but if you'd prefer to run it locally:

```
git clone https://github.com/hakluke/hakrawler
cd hakrawler
sudo docker build -t hakluke/hakrawler .
sudo docker run --rm -i hakluke/hakrawler --help
```
### Kali Linux: Using apt

Note: This will install an older version of hakrawler without all the features, and it may be buggy. I recommend using one of the other methods.

```sh
sudo apt install hakrawler
```

Then, to run hakrawler:

```
echo https://www.google.com | docker run --rm -i hakluke/hakrawler -subs
```

## Boundary-aware crawling (depth / scope / rules / stats)

The original crawler applies depth and domain checks inside the HTTP layer,
after a request goroutine has already been scheduled, and it deduplicates on
the raw URL string — so `?a=1&b=2`, `?b=2&a=1` and `#frag` variants are all
fetched separately, with no accounting of what was dropped or why.

`-boundary` switches on an alternative crawl engine that checks every link
**before it is enqueued** (no goroutine or HTTP request is ever created for a
refused link), canonicalises URLs through one shared normaliser, and reports
end-of-run statistics. Every other flag keeps its meaning; without
`-boundary` the program behaves exactly as before.

```
cat urls.txt | hakrawler -boundary -d 3 -scope host -stats run.json
```

### New flags (all inert without `-boundary`)

| Flag | Meaning |
|------|---------|
| `-boundary` | Enable pre-enqueue guards, canonical dedup and the stats report. |
| `-scope host\|domain\|whitelist` | `host` (default): only the seed's exact host. `domain`: the seed's registrable domain (eTLD+1) and any subdomain. `whitelist`: only hosts given by `-whitelist`. |
| `-whitelist a.com,b.com` | Comma-separated hosts for `-scope=whitelist`. An entry also admits its subdomains (`a.com` admits `api.a.com`). |
| `-rules name=regex,...` | Custom exclusion rules, evaluated before the built-ins. A bare regex gets an automatic `user-rule-N` name. Invalid regex fails at startup. |
| `-stats path` | Write the JSON report to `path` atomically (temp file + `rename`, so a crash never leaves a half-written file). Without it, the JSON is printed to stderr. |

The legacy `-subs` flag is accepted and mapped to `-scope=domain`. Combining
`-subs` with an explicit, different `-scope` is a startup error.

### Check order and the four counters

For every extracted link the guard runs, in order:

1. **depth** — the link's would-be depth is parent depth + 1; over the limit
   → `depth_skipped`;
2. **non-HTTP scheme** (`mailto:`, `tel:`, `javascript:`, …) → `rule_excluded`
   with rule name `non-http-scheme`;
3. **scope** — host (and `-i` path prefix) outside the allowed set →
   `scope_excluded`;
4. **exclusion rules** — built-in `static-asset` (css/js/images/fonts/
   archives/…) and `junk-path`, plus any `-rules` entries →
   `rule_excluded`, broken down per rule in `rule_hits`;
5. **dedup** — one canonical URL is fetched once; later writings →
   `duplicate_skipped`.

The report's buckets partition the candidate set exactly:

```
candidate_links == enqueued + depth_skipped + scope_excluded
                 + rule_excluded + duplicate_skipped
```

The report also prints this invariant line so it can be checked on real runs.

### URL canonicalisation (single definition)

`NormalizeURL` is the one place identity is defined; seed validation, the
enqueue guard, dedup and `-u` output dedup all call it. It: strips
fragments, lowercases scheme/host, removes default ports (`:80`/`:443`) and
sorts query parameters (repeated params included). It deliberately preserves
path case, percent-encoding and trailing slashes, because rewriting those can
point at a different server-side resource.

### Determinism and streaming

Depth levels are processed breadth-first. Pages within a level are fetched
concurrently (honouring `-t`), but output is sorted by `(page URL, in-page
element order, attribute value)` and flushed one level at a time, so repeated
runs over the same site produce byte-identical output regardless of goroutine
scheduling — and results are streamed to stdout rather than buffered whole.

### Failure behaviour

- One page failing (peer reset, truncated/malformed body, parse error) is
  counted in `fetch_errors`/`parse_errors` and skipped; all other pages
  continue.
- A seed outside the configured scope is rejected **at startup**, with the
  reason printed and recorded in `seed_rejections`; exit code `2` when every
  seed was rejected, `1` for bad flags or output failures, `0` otherwise.
- A stats-file write is atomic, so no partial stats file is ever left behind.

### Known limitations

- Scope and rules are evaluated on the **pre-redirect** URL. A page that
  responds with a redirect to an out-of-scope host is still fetched; the
  links extracted from that response are then guarded normally. This matches
  the legacy collector's redirect handling.
- Path case is not folded (see canonicalisation above); links differing only
  in path case are treated as distinct resources.
- Stats describe the run as executed; the stdout line format itself is
  unchanged.

## Command-line options
```
Usage of hakrawler:
  -boundary
    	Enable pre-enqueue boundary guards (depth/scope/rules), canonical dedup and end-of-run stats.
  -d int
    	Depth to crawl. (default 2)
  -dr
    	Disable following HTTP redirects.
  -h string
    	Custom headers separated by two semi-colons. E.g. -h "Cookie: foo=bar;;Referer: http://example.com/"
  -i	Only crawl inside path
  -insecure
    	Disable TLS verification.
  -json
    	Output as JSON.
  -proxy string
    	Proxy URL. E.g. -proxy http://127.0.0.1:8080
  -rules string
    	Comma-separated custom exclusion rules "name=regex" (boundary mode); evaluated before built-ins.
  -s	Show the source of URL based on where it was found. E.g. href, form, script, etc.
  -scope string
    	Boundary scope mode: host, domain or whitelist. (default "host")
  -size int
    	Page size limit, in KB. (default -1)
  -stats string
    	Write the JSON stats report to this path atomically; default prints JSON stats to stderr.
  -subs
    	Include subdomains for crawling (mapped to -scope=domain in boundary mode).
  -t int
    	Number of threads to utilise. (default 8)
  -timeout int
    	Maximum time to crawl each URL from stdin, in seconds. (default -1)
  -u	Show only unique urls.
  -w	Show at which link the URL is found.
  -whitelist string
    	Comma-separated hosts for -scope=whitelist.
```

---

Built by [hakluke](https://hakluke.com) (Luke Stephens), founder and CEO of [Haksec](https://haksec.io), a group of cybersecurity companies:

- [HackerContent](https://hackercontent.com) — content marketing and social media for cybersecurity companies
- [Ironbark Cyber](https://ironbarkcyber.com) — cybersecurity consultancy for penetration testing, social engineering and OSINT
- [TRIAGERS™](https://triagers.com) — outsourced bug bounty and vulnerability disclosure (VDP) triage

I write about hacking and the security industry at [hakluke.com](https://hakluke.com).
