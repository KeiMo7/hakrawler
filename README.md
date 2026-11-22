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

## Resumable crawling (survive reboots and interruptions)

Long crawls can now persist their progress. This is **opt-in** with `-resume`;
without it, behaviour, output and exit codes are unchanged.

```sh
# start a resumable crawl; progress is kept in ./crawl-state
cat urls.txt | hakrawler -resume ./crawl-state

# if the machine reboots or the process is killed, rerun the same command:
# already-fetched addresses are skipped, failed ones are retried, and the
# final link set is identical to a single uninterrupted crawl
cat urls.txt | hakrawler -resume ./crawl-state
```

Key properties:

- URL identity is the normalized address (fragment dropped, query parameters
  sorted, host lower-cased), so different spellings of the same address are
  fetched only once.
- Progress is written atomically with per-line checksums; a half-written
  record from a hard kill is detected and reported with recovery steps.
- Adding new seed URLs merges them into the queue and reuses finished work;
  replacing/removing old seeds is refused to avoid mis-attribution.
- A queue snapshot (`<dir>/queue-snapshot.json`) shows completed/queued/failed
  counts, the pending addresses and each failure's type, so you can see where
  a crawl is stuck.
- `-resume-sync group` (default) batches disk flushes and adds no measurable
  overhead; `-resume-sync strict` flushes every record for zero-loss-on-kill at
  a higher cost.

See [RESUME.md](RESUME.md) for the full design, flags, durability/performance
trade-offs, failure messages and compatibility boundaries.

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

## Command-line options
```
Usage of hakrawler:
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
  -resume string
    	Enable resumable crawling and store progress in this state directory. Re-run with the same directory to continue.
  -resume-snapshot string
    	Path for an exportable JSON snapshot of the queue/progress (defaults to <resume-dir>/queue-snapshot.json).
  -resume-snapshot-interval duration
    	How often the queue snapshot is refreshed while resuming (0 disables periodic snapshots). (default 5s)
  -resume-sync string
    	Durability mode for the journal: 'group' batches fsyncs (fast, a crash redoes at most one sync window), 'strict' fsyncs every record (slowest, zero record loss on SIGKILL). (default "group")
  -resume-sync-window duration
    	Max batching delay for -resume-sync=group before records are forced to disk. (default 50ms)
  -s	Show the source of URL based on where it was found. E.g. href, form, script, etc.
  -size int
    	Page size limit, in KB. (default -1)
  -subs
    	Include subdomains for crawling.
  -t int
    	Number of threads to utilise. (default 8)
  -timeout int
    	Maximum time to crawl each URL from stdin, in seconds. (default -1)
  -u	Show only unique urls.
  -w	Show at which link the URL is found.
```

---

Built by [hakluke](https://hakluke.com) (Luke Stephens), founder and CEO of [Haksec](https://haksec.io), a group of cybersecurity companies:

- [HackerContent](https://hackercontent.com) — content marketing and social media for cybersecurity companies
- [Ironbark Cyber](https://ironbarkcyber.com) — cybersecurity consultancy for penetration testing, social engineering and OSINT
- [TRIAGERS™](https://triagers.com) — outsourced bug bounty and vulnerability disclosure (VDP) triage

I write about hacking and the security industry at [hakluke.com](https://hakluke.com).
