# reddit-radar-collector

Reddit public RSS → JSONL feed collector, for the Opportunity Radar bridge
gate. **Read-only**: no Reddit login, no Reddit API, no proxies, no LLM calls,
no scoring, no posting. This is a gate prototype — NOT the formal Radar
integration, and it does not touch the existing Radar.

```
Reddit public RSS (keyless)
  └─ collector/reddit.py    fetch layer extracted from subscope (MIT)
  └─ collector/run.py       one collection cycle
       ├─ data/latest_posts.jsonl   24h rolling feed, dedup by post id
       ├─ data/state.json           per-sub cursors + run bookkeeping
       └─ data/health.json          COLLECTOR_OK / COLLECTOR_STALE / REDDIT_SOURCE_DEGRADED
```

## Upstream provenance

`collector/reddit.py` is a minimal extraction of
[subscope](https://github.com/dancolta/subscope) `engine/subscope/lib/reddit.py`
@ commit `cf45ffd8956d0d24bbaaf4eaa5d7f0ecc293cb27`, MIT, © Dan Colta.
Preserved behaviors: UA-only header discipline (Reddit 403s keyless RSS GETs
that carry an `Accept` header), per-IP throttle, x-ratelimit-aware pacing,
429 retry with Retry-After / reset / full-window backoff, dual-host failover
(www → old.reddit on 403/5xx/network, never on 429), robust Atom parsing,
per-sub cursor delta. Removed: scoring, keywords, CLI, config system, SQLite,
search, per-post/per-user fetchers, batching, JSON surface. See the header of
`collector/reddit.py` for the full derivation record. License: MIT (see
`LICENSE`).

## Data contract

`data/latest_posts.jsonl` — one JSON object per line, exactly these keys:

```json
{"id": "t3_x", "subreddit": "...", "title": "...", "author": "...",
 "published_at": "YYYY-MM-DDTHH:MM:SSZ", "url": "...", "body": "...",
 "fetched_at": "YYYY-MM-DDTHH:MM:SSZ", "source": "reddit_rss"}
```

Only fields Reddit's RSS actually provides are emitted. RSS does not carry
scores or comment counts, so none appear. The feed keeps the most recent 24
hours (rolling, by `published_at`), deduped by `id`, newest first. An empty
feed or an empty fetch is never interpreted downstream as "no opportunities" —
check `health.json`.

`data/state.json` — `last_successful_run`, `subreddit_cursors` (per-sub newest
post id), `seen_count` (unique ids in the current feed window), plus
bookkeeping (`total_seen_count` may undercount if a run is killed between the
feed write and the state write; `seen_count` is always exact).

`data/health.json` — `status` is exactly one of:

| status | rule |
|---|---|
| `COLLECTOR_OK` | last successful collection ≤ 45 min ago |
| `COLLECTOR_STALE` | last successful collection > 45 min ago |
| `REDDIT_SOURCE_DEGRADED` | RSS failing on consecutive full runs (≥2), or an HTTP 403 fingerprint block observed with all subs failing |

## Reliability design

- **Dedup is structural**: the feed is rebuilt each run as the id-union of the
  existing feed and freshly fetched posts. Duplicate ids are dropped, never
  rewritten.
- **Kill-safe**: every file is written via temp-file + `os.replace`. If the
  process dies after the feed write but before the state write, the next run
  refetches and the id-union absorbs the overlap — the feed can never
  accumulate duplicates.
- **Failure isolation**: each subreddit is fetched in its own try/except; one
  broken sub never blocks the others. A fully failed run leaves the existing
  feed untouched (no false "empty market" signal) and increments the
  consecutive-failure counter that drives `REDDIT_SOURCE_DEGRADED`.
- **Rate-limit safety**: extracted pacing/backoff means a run of 6 subs costs
  ~6 GETs spread over ~5–8 minutes of server-instructed pacing, well inside
  the keyless RSS budget (~100 req/10 min per IP, ~1 req/min observed steady
  state).

## Usage

```bash
python3 collector/run.py                  # full sweep (the scheduled path)
python3 collector/run.py --only-sub SaaS  # single-sub cycle, commits incrementally
python3 -m unittest discover -s tests     # offline test suite (no network)
```

Requires Python ≥ 3.10. Dependencies: none (certifi is used if present, for
CA bundling). No LLM, no database, no Docker.

## GitHub Actions bridge (cost-gated)

`.github/workflows/collect.yml` runs the full sweep every 20 minutes and
commits `data/` back to the repo, so downstream reads the feed from:

```
https://raw.githubusercontent.com/<owner>/<repo>/<branch>/data/latest_posts.jsonl
https://raw.githubusercontent.com/<owner>/<repo>/<branch>/data/health.json
```

or via the Contents API:

```
https://api.github.com/repos/<owner>/<repo>/contents/data/latest_posts.jsonl
```

**Cost gate (verified 2026-09 against GitHub's pricing docs):**

- **Public repo: $0.** Actions is free for public repositories on standard
  runners (no minute cap), and this workflow uses only `ubuntu-latest`,
  first-party actions, and the built-in `GITHUB_TOKEN`.
- **Private repo: can cost money.** Free tier is 2,000 min/month; at every
  20 minutes with ~5–8 min per run this schedule needs ~5,000–11,000
  min/month. **Do not enable this workflow on a private repo.**
- Scheduler caveat: GitHub may delay or skip scheduled runs under load; never
  assume on-time execution. Real run times are recorded in
  `state.json` (`last_run_at`) and `health.json` (`run_at`), and every feed
  line carries `fetched_at`. Scheduled workflows on a quiet repo are disabled
  after 60 days of no activity — the data commits themselves keep the repo
  active.
- A 12-minute job timeout bounds the worst case (retry backoff loops).

## Known limitations

- RSS is Reddit's only keyless surface left (anonymous `.json` and HTML are
  edge-blocked as of 2026). If Reddit closes RSS, the collector degrades to
  `REDDIT_SOURCE_DEGRADED` instead of silently showing an empty market.
- Feeds are single ~25-item pages: more than 25 posts per sub per cycle can
  miss older ones (acceptable for radar use).
- No scores / comment counts / nested comments (RSS does not carry them).
- `body` is plain text capped at 1000 chars (upstream extraction behavior).

## Status

Bridge-gate prototype. The formal Opportunity Radar integration is **not**
authorized by this build and remains a separate decision.
