"""Reddit RSS fetch layer for Opportunity Radar (keyless, accountless).

DERIVATION / LICENSE
--------------------
Extracted from subscope: https://github.com/dancolta/subscope
Upstream file: engine/subscope/lib/reddit.py
Upstream commit: cf45ffd8956d0d24bbaaf4eaa5d7f0ecc293cb27 (2026-08-12)
Upstream license: MIT, Copyright (c) 2026 Dan Colta (see LICENSE at repo root).

This is a MINIMAL extraction for a read-only RSS collector. Logic is preserved
verbatim except where noted under "Modifications vs upstream" below. It is NOT
the whole subscope project: no scoring, no buyer-intent keywords, no CLI, no
config system, no LLM, no SQLite, no author vetting, no search, no per-post /
per-user fetchers, no JSON surface.

What is kept (required behaviors):
  - UA-only header discipline (Reddit's edge 403s keyless RSS GETs that carry
    any explicit Accept header; UA-only returns 200)
  - per-IP throttle + x-ratelimit-remaining/reset-aware pacing
  - 429 retry with Retry-After / x-ratelimit-reset / full-window backoff
  - dual-host failover www.reddit.com -> old.reddit.com on 403/5xx/network
    (never on 429; the bucket is shared per IP)
  - robust Atom parsing (entities, SC_OFF chrome, submitted-by footer)
  - per-sub cursor delta (fetch_delta / delta_from_posts)

Modifications vs upstream:
  1. USER_AGENT string replaced with this collector's own identity.
  2. Removed unused surface: fetch_json, parse_post, fetch_post, fetch_user_*,
     fetch_search, batched multi-sub feed (plan_sub_batches / fetch_multi_new /
     fetch_new_batched / prime_new_cache / _PREFETCH), canonical JSON tests
     helpers, _safe_username. fetch_delta now calls fetch_subreddit_new
     directly instead of reading a prefetch cache.
  3. fetch_subreddit_new / fetch_delta return [] on failure (upstream
     behavior kept); reachability is reported via get_fetch_stats().
  4. Exposed is_rate_limited() (upstream private helper) for health reporting.

RSS does NOT carry score, num_comments, upvote_ratio, or locked state. Those
default (score=0, num_comments=0, upvote_ratio=None, locked=False) and are
DROPPED by the run.py output layer — no fabricated data is emitted.
"""
from __future__ import annotations

import html
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from typing import Any

try:
    import certifi
    _SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    _SSL_CONTEXT = ssl.create_default_context()


# ─── Constants (upstream values preserved) ────────────────────────────

# Modified vs upstream: honest derivative identity instead of subscope's UA.
USER_AGENT = "opportunity-radar-collector/0.1 (Reddit keyless RSS reader; derived from subscope https://github.com/dancolta/subscope)"
MAX_RETRIES = 3

# Rate-limit discipline (upstream). Reddit's keyless RSS surface enforces a
# per-IP token bucket (~100 req / 10 min, surfaced via x-ratelimit-* headers;
# observed steady state ~1 request per ~60s window). We pace requests instead
# of bursting.
MIN_REQUEST_INTERVAL = 1.6
RATELIMIT_REMAINING_FLOOR = 2.0
MAX_RATELIMIT_PAUSE = 60.0

# Ordered RSS hosts for the dual-host 403 failover. www is primary; old.reddit
# is the failover when www returns 403/5xx/network (NOT on 429, which is a
# per-IP bucket shared across hosts). No HTML scrape, no auth host ever.
RSS_HOSTS = ("www.reddit.com", "old.reddit.com")

# Atom namespace used by Reddit RSS feeds.
_ATOM_NS = "{http://www.w3.org/2005/Atom}"


# ─── Logging ──────────────────────────────────────────────────────────

def _log(msg: str) -> None:
    sys.stderr.write(f"[reddit] {msg}\n")
    sys.stderr.flush()


# ─── Fetch reachability stats (upstream, trimmed) ─────────────────────

_FETCH_STATS = {"ok": 0, "failed": 0, "rate_limited": 0, "fallback_used": 0,
                "http_403": 0}  # http_403: local extension for health reporting
_RATE_STATE = {"drained": False}

# Per-run Reddit GET budget. None means unlimited.
_BUDGET: dict[str, int | None] = {"limit": None, "used": 0}

# Wall-clock timestamp of the last Reddit GET, for proactive inter-request
# spacing. Module-level so spacing holds across subs within a run.
_last_request_at = 0.0


def reset_fetch_stats() -> None:
    """Zero the per-run feed counters, rate state, and GET budget."""
    global _last_request_at
    _FETCH_STATS["ok"] = 0
    _FETCH_STATS["failed"] = 0
    _FETCH_STATS["rate_limited"] = 0
    _FETCH_STATS["fallback_used"] = 0
    _FETCH_STATS["http_403"] = 0
    _RATE_STATE["drained"] = False
    _BUDGET["limit"] = None
    _BUDGET["used"] = 0
    _last_request_at = 0.0


def set_request_budget(limit: int | None) -> None:
    """Cap Reddit GETs for this run (None = unlimited)."""
    _BUDGET["limit"] = limit
    _BUDGET["used"] = 0


def requests_used() -> int:
    return int(_BUDGET["used"])


def get_fetch_stats() -> dict[str, int]:
    return dict(_FETCH_STATS)


def is_rate_limited() -> bool:
    """True once a 429 landed or the bucket reported at/under the floor."""
    return bool(_RATE_STATE["drained"])


# ─── Throttle + header pacing (upstream verbatim) ─────────────────────

def _sleep(seconds: float) -> None:
    """Indirection over time.sleep so tests can assert spacing without waiting."""
    if seconds > 0:
        time.sleep(seconds)


def _throttle() -> None:
    """Proactively space Reddit GETs at least MIN_REQUEST_INTERVAL apart."""
    global _last_request_at
    now = time.monotonic()
    elapsed = now - _last_request_at
    if _last_request_at and elapsed < MIN_REQUEST_INTERVAL:
        _sleep(MIN_REQUEST_INTERVAL - elapsed)
    _last_request_at = time.monotonic()


def _ratelimit_pause_from_headers(headers: Any) -> None:
    """Read x-ratelimit-remaining / x-ratelimit-reset off a 200 and pause until
    the bucket refills, so the NEXT GET does not 429.

    This runs on a SUCCESSFUL response, so it is pacing, not failure. Reddit's
    keyless bucket reports remaining=0 on the first 200 of every window, so
    treating that as terminal would end every run after one request.
    """
    if headers is None:
        _RATE_STATE["drained"] = False
        return
    remaining = _header_float(headers, "x-ratelimit-remaining")
    if remaining is not None and remaining <= RATELIMIT_REMAINING_FLOOR:
        reset = _header_float(headers, "x-ratelimit-reset")
        pause = min(reset, MAX_RATELIMIT_PAUSE) if reset and reset > 0 else MIN_REQUEST_INTERVAL
        _log(f"ratelimit low (remaining={remaining}), pacing {pause:.1f}s until reset")
        _sleep(pause)
    # A 200 means the host is serving us. Clear any drained flag left by an
    # earlier 429 that a retry has now recovered from.
    _RATE_STATE["drained"] = False


def _header_float(headers: Any, name: str) -> float | None:
    """Parse a numeric header value to float, or None if absent/unparseable."""
    if headers is None:
        return None
    raw = headers.get(name)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


# ─── Sub name safety (upstream) ───────────────────────────────────────

def _normalize_sub(sub: str) -> str:
    """Strip an optional 'r/' or '/r/' prefix and surrounding slashes/space.

    NOT lstrip('r/'): that strips every leading 'r' and '/' as a character
    class, mangling subs that start with 'r'. Callers URL-encode the result
    with quote(safe='') so '/' and '.' cannot escape the path segment.
    """
    s = (sub or "").strip()
    if s.startswith("/r/"):
        s = s[3:]
    elif s.startswith("r/"):
        s = s[2:]
    return s.strip("/").strip()


# ─── URL canonicalization (upstream verbatim) ─────────────────────────

def canonical_url(reddit_post_data: dict[str, Any]) -> str:
    """Canonicalize a Reddit post URL to https://reddit.com/comments/<t3_id>/.

    Post IDs are globally unique on Reddit; the subreddit slug is unnecessary
    for the canonical form. Strips host variants (old., np., www., m.), query
    strings, and trailing slashes.
    """
    raw_id = str(reddit_post_data.get("id") or reddit_post_data.get("name") or "").strip()
    raw_id = re.sub(r"^t3_", "", raw_id)
    if not raw_id:
        permalink = str(reddit_post_data.get("permalink") or "")
        m = re.search(r"/comments/([a-z0-9]+)", permalink, re.I)
        if not m:
            return ""
        raw_id = m.group(1)
    return f"https://reddit.com/comments/{raw_id}/"


# ─── Core XML GET with 429 backoff (upstream verbatim) ────────────────

def _fetch_xml_attempt(url: str, timeout: int = 15) -> tuple[str, ET.Element | None]:
    """Single-URL RSS/Atom GET. Returns one of:
      ("ok", root) | ("rate_limited", None) | ("failed", None) | ("budget", None)

    Does the per-request work (throttle spacing, 429 retry/backoff, x-ratelimit
    header pacing) but does NOT touch the per-sub OUTCOME counters. The CALLER
    decides the outcome, so a www 403 that then succeeds on old.reddit can be
    counted once (as ok) instead of as failed+ok.
    """
    # User-Agent ONLY. Do NOT send an explicit Accept header: as of 2026-06-01
    # Reddit's edge (Fastly) 403s any keyless RSS GET that carries an Accept
    # header from a non-browser TLS client, www and old alike, regardless of
    # the Accept value. The same URL with UA-only returns 200. Omitting Accept
    # sends NO Accept header at all (urllib adds no default).
    headers = {"User-Agent": USER_AGENT}
    req = urllib.request.Request(url, headers=headers)

    for attempt in range(MAX_RETRIES):
        # Retries are real GETs against the same per-IP bucket, so they are
        # charged to the budget too. "budget" is distinct from "failed": no
        # request was made, so it must not count as a reachability failure.
        if _BUDGET["limit"] is not None and _BUDGET["used"] >= int(_BUDGET["limit"]):
            _log("request budget spent, skipping GET")
            return ("budget", None)
        _BUDGET["used"] = int(_BUDGET["used"] or 0) + 1
        _throttle()  # proactive inter-request spacing, every attempt
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CONTEXT) as resp:
                body = resp.read().decode("utf-8")
                root = ET.fromstring(body)
                # Header-aware pacing: if the bucket is nearly empty, pause now
                # (until reset, capped) so the NEXT GET does not 429.
                _ratelimit_pause_from_headers(getattr(resp, "headers", None))
                return ("ok", root)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                # The bucket is drained. Flag it so the caller stops bursting.
                _RATE_STATE["drained"] = True
                delay = _retry_after_delay(e, attempt)
                _log(f"429 rate-limited, retry {attempt + 1}/{MAX_RETRIES} after {delay:.1f}s")
                if attempt < MAX_RETRIES - 1:
                    _sleep(delay)
                    continue
                _log("429 retries exhausted")
                return ("rate_limited", None)
            elif e.code in (403, 404):
                if e.code == 403:
                    _FETCH_STATS["http_403"] += 1
                _log(f"HTTP {e.code}: {url}")
                return ("failed", None)
            else:
                _log(f"HTTP {e.code}: {e.reason}")
                return ("failed", None)
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            _log(f"network error: {e}")
            return ("failed", None)
        except ET.ParseError as e:
            _log(f"XML parse error: {e}")
            return ("failed", None)
    return ("failed", None)


def _retry_after_delay(err: urllib.error.HTTPError, attempt: int) -> float:
    """Backoff delay for a 429. Prefer Retry-After, then x-ratelimit-reset, else
    wait out a full window. Always capped at MAX_RATELIMIT_PAUSE so a bogus
    header value can never hang the run.

    The no-header fallback is a FULL WINDOW, not exponential-from-2s. The
    bucket refills once per ~60s window, so 2s/4s/8s retries all land inside
    the window that just rejected us and are guaranteed to fail.
    """
    hdrs = getattr(err, "headers", None)
    retry_after = _header_float(hdrs, "Retry-After")
    if retry_after is not None and retry_after > 0:
        delay = retry_after
    else:
        reset = _header_float(hdrs, "x-ratelimit-reset")
        delay = reset if reset is not None and reset > 0 else MAX_RATELIMIT_PAUSE
    return min(delay, MAX_RATELIMIT_PAUSE)


# ─── Dual-host resilient fetch (upstream verbatim) ────────────────────

def fetch_xml_resilient(path: str, timeout: int = 15) -> ET.Element | None:
    """Fetch an RSS/Atom path across RSS_HOSTS with 403/5xx/network failover.

    `path` is host-relative and begins with '/', e.g. '/r/saas/new/.rss?limit=25'.
    Per-host precedence:
      - ok            -> count one ok (+ fallback_used if a non-primary host
                         served it) and return the parsed root.
      - rate_limited  -> the per-IP token bucket is drained; it is SHARED
                         across hosts, so do NOT advance. Return None.
      - failed        -> try the next host.
    All hosts failed -> count one failed, return None.

    Exactly ONE outcome counter is incremented per call, so
    ok + failed + rate_limited == attempts-attempted holds with failover.
    """
    for i, host in enumerate(RSS_HOSTS):
        url = f"https://{host}{path}"
        status, root = _fetch_xml_attempt(url, timeout=timeout)
        if status == "ok":
            _FETCH_STATS["ok"] += 1
            if i > 0:
                _FETCH_STATS["fallback_used"] += 1
                _log(f"served via failover host {host}: {path}")
            return root
        if status == "rate_limited":
            _RATE_STATE["drained"] = True
            _FETCH_STATS["rate_limited"] += 1
            return None
        if status == "budget":
            # Budget spent: do not try the failover host (it draws from the
            # same per-IP bucket) and do not count a reachability failure.
            return None
        # failed: try the next host
    _FETCH_STATS["failed"] += 1
    return None


# ─── Atom parsing (upstream verbatim) ─────────────────────────────────

def _atom_text(entry: ET.Element, tag: str) -> str:
    """Return stripped text of the first <tag> child in the Atom namespace, or ''."""
    el = entry.find(f"{_ATOM_NS}{tag}")
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _parse_iso8601_to_epoch(ts: str) -> int:
    """Parse an ISO8601 timestamp (e.g. '2026-05-29T10:14:46+00:00') to epoch int.

    Returns 0 on empty/unparseable input. Handles a trailing 'Z' (UTC) which
    older Python's fromisoformat rejects.
    """
    if not ts:
        return 0
    cleaned = ts.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        from datetime import datetime
        return int(datetime.fromisoformat(cleaned).timestamp())
    except (ValueError, OverflowError):
        return 0


def _clean_atom_body(raw_html: str) -> str:
    """Strip Reddit's RSS content chrome and return a plain-text body, capped 1000.

    Reddit wraps post bodies in `<!-- SC_OFF -->...<!-- SC_ON -->` and appends a
    'submitted by /u/x [link] [comments]' footer. We HTML-unescape, drop the
    footer, strip tags to text, and cap to 1000 chars.
    """
    if not raw_html:
        return ""
    text = html.unescape(raw_html)
    text = re.sub(r"<!--\s*SC_O(FF|N)\s*-->", "", text)
    text = re.sub(r"submitted by\b.*$", "", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)  # second pass for entities revealed after tag strip
    text = re.sub(r"\s+", " ", text).strip()
    return text[:1000]


def parse_atom_entry(entry: ET.Element) -> dict[str, Any] | None:
    """Normalize a Reddit Atom <entry> into the post dict shape.

    Returns None on bad data (missing id, missing permalink). RSS does not
    carry engagement metrics, so score/num_comments default to 0 and
    locked/removed to False (the feed lists live posts only).

    Output keys: id, subreddit, title, url, canonical_url, author, created_utc,
    score, num_comments, body, upvote_ratio, removed, locked, over_18,
    is_crosspost.
    """
    # Permalink: the <link href=...> of the entry.
    permalink = ""
    for link in entry.findall(f"{_ATOM_NS}link"):
        href = link.get("href")
        if href:
            permalink = href.strip()
            break
    if not permalink or "/comments/" not in permalink:
        return None

    # Post id: <id>t3_xxxx</id>, falling back to the permalink.
    raw_id = _atom_text(entry, "id")
    post_id = re.sub(r"^t3_", "", raw_id).strip()
    if not post_id:
        m = re.search(r"/comments/([a-z0-9]+)", permalink, re.I)
        if not m:
            return None
        post_id = m.group(1)
    # Guard: an <id> from a comment feed is t1_ (a comment), not a post. Only
    # keep ids that resolve from the t3 namespace or the post permalink.
    if not re.fullmatch(r"[A-Za-z0-9]+", post_id):
        return None

    canon = canonical_url({"id": post_id, "permalink": permalink})
    if not canon:
        return None

    # Subreddit: <category term="SaaS"> on the entry.
    subreddit = ""
    cat = entry.find(f"{_ATOM_NS}category")
    if cat is not None:
        subreddit = (cat.get("term") or "").strip()

    # Author: <author><name>/u/Name</name></author>.
    author = "[deleted]"
    author_el = entry.find(f"{_ATOM_NS}author/{_ATOM_NS}name")
    if author_el is not None and author_el.text:
        author = author_el.text.strip().lstrip("/").removeprefix("u/") or "[deleted]"

    title = _atom_text(entry, "title")
    published = _atom_text(entry, "published") or _atom_text(entry, "updated")
    created_utc = _parse_iso8601_to_epoch(published)

    content_el = entry.find(f"{_ATOM_NS}content")
    body = _clean_atom_body(content_el.text if content_el is not None else "")

    return {
        "id": post_id,
        "subreddit": subreddit,
        "title": title,
        "url": permalink,
        "canonical_url": canon,
        "author": author,
        "created_utc": created_utc,
        "score": 0,
        "num_comments": 0,
        "body": body,
        "upvote_ratio": None,
        "removed": False,
        "locked": False,
        "over_18": False,
        "is_crosspost": False,
    }


# ─── Subreddit feeds + per-sub cursor delta (upstream, prefetch removed) ──

def fetch_subreddit_new(sub: str, limit: int = 25,
                        timeout: int = 15) -> list[dict[str, Any]]:
    """Fetch /r/<sub>/new/.rss and return normalized posts (newest-first).

    The Atom feed is a single page of the most recent ~25 items with no
    cursor, so there is no pagination contract. Returns [] on any fetch/parse
    failure (check get_fetch_stats() to distinguish unreachable from empty).
    """
    encoded = urllib.parse.quote(_normalize_sub(sub), safe="")
    path = f"/r/{encoded}/new/.rss?limit={int(limit)}"

    root = fetch_xml_resilient(path, timeout=timeout)
    if root is None:
        return []

    posts: list[dict[str, Any]] = []
    for entry in root.findall(f"{_ATOM_NS}entry"):
        p = parse_atom_entry(entry)
        if p:
            posts.append(p)
    return posts


def delta_from_posts(posts: list[dict[str, Any]], last_seen_id: str | None,
                     max_limit: int = 50) -> list[dict[str, Any]]:
    """Apply the delta cut to an already-fetched, newest-first post list.

    Stop at last_seen_id, skip removed/locked, cap at max_limit.
    """
    collected: list[dict[str, Any]] = []
    for p in posts:
        if last_seen_id and p["id"] == last_seen_id:
            break
        if p["removed"] or p["locked"]:
            continue
        collected.append(p)
        if len(collected) >= max_limit:
            break
    return collected


def fetch_delta(sub: str, last_seen_id: str | None,
                max_limit: int = 50) -> list[dict[str, Any]]:
    """Delta scan: posts newer than last_seen_id from /r/<sub>/new/.rss.

    Posts already covered by last_seen_id are never returned, which is the
    per-sub dedup the caller's cursor relies on.
    """
    posts = fetch_subreddit_new(sub, limit=min(max_limit, 100))
    return delta_from_posts(posts, last_seen_id, max_limit=max_limit)
