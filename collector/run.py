#!/usr/bin/env python3
"""Opportunity Radar bridge collector — run one collection cycle.

Pipeline (this build is a GATE prototype, NOT the formal Radar integration):

    Reddit public RSS  ->  collector/reddit.py (keyless fetch layer)
                       ->  data/latest_posts.jsonl   (24h rolling feed)
                       ->  data/state.json           (cursors + run bookkeeping)
                       ->  data/health.json          (COLLECTOR_OK / COLLECTOR_STALE
                                                      / REDDIT_SOURCE_DEGRADED)

Design invariants:
  - Dedup by post id: the feed is REBUILT each run as the id-union of the
    existing feed and the newly fetched posts, then filtered to a rolling 24h
    window. Duplicate ids are dropped, never written. This makes a killed run
    safe: if the process dies after the feed write but before the state write,
    the next run refetches the same posts and the id-union absorbs them.
  - Atomic writes: every output file is written to a temp file in the same
    directory then os.replace()d, so a crash never leaves a partial file.
  - Failure isolation: each subreddit is fetched inside its own try/except;
    one sub failing never blocks the others.
  - Full failure never truncates the feed: if every sub fails, the existing
    latest_posts.jsonl is left untouched (an empty or failed fetch is NOT
    reported as "no opportunities").
  - No LLM calls, no scoring, no Reddit auth, no fabricated fields: RSS does
    not carry scores/comment counts, so none are emitted.

Usage:
    python3 collector/run.py                 # normal cycle
    python3 collector/run.py --subs a,b      # override subreddit list
    python3 collector/run.py --budget 12     # cap Reddit GETs this run
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reddit as rss  # noqa: E402  (extracted subscope fetch layer, MIT)

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
FEED_PATH = DATA_DIR / "latest_posts.jsonl"
STATE_PATH = DATA_DIR / "state.json"
HEALTH_PATH = DATA_DIR / "health.json"

DEFAULT_SUBS = ["smallbusiness", "Entrepreneur", "ecommerce",
                "shopify", "EtsySellers", "SaaS"]
WINDOW_HOURS = 24          # rolling retention for latest_posts.jsonl
STALE_AFTER_MIN = 45       # health: last success older than this -> COLLECTOR_STALE
DEGRADE_AFTER_CONSEC = 2   # consecutive full failures -> REDDIT_SOURCE_DEGRADED
FETCH_LIMIT = 25           # RSS single-page cap (~25 items)

REQUIRED_LINE_KEYS = ("id", "subreddit", "title", "author", "published_at",
                      "url", "body", "fetched_at", "source")

HEALTH_OK = "COLLECTOR_OK"
HEALTH_STALE = "COLLECTOR_STALE"
HEALTH_DEGRADED = "REDDIT_SOURCE_DEGRADED"


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_z(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


# ─── Atomic file IO ───────────────────────────────────────────────────

def atomic_write(path: Path, text: str) -> None:
    """Write via temp file + os.replace so a crash never leaves a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".tmp-", suffix=path.name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ─── State ────────────────────────────────────────────────────────────

def default_state() -> dict:
    return {
        "last_successful_run": None,   # ISO Z; None = never succeeded
        "subreddit_cursors": {},       # sub -> newest post id (t3_, no prefix) seen
        "seen_count": 0,               # unique post ids in the current feed window
        "total_seen_count": 0,         # cumulative unique ids ever observed
        "consecutive_full_failures": 0,
        "last_run_at": None,
        "last_run_result": None,
    }


def load_state() -> dict:
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default_state()
    base = default_state()
    base.update({k: v for k, v in state.items() if k in base})
    return base


# ─── Feed IO ──────────────────────────────────────────────────────────

def load_feed() -> list[dict]:
    """Parse the existing JSONL feed. A corrupt line is dropped, not fatal."""
    posts: list[dict] = []
    if not FEED_PATH.exists():
        return posts
    for line in FEED_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            posts.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return posts


def to_feed_line(post: dict, fetched_at: str) -> dict:
    """Map the fetch-layer post dict to the spec'd feed line. Only fields RSS
    actually provides are emitted — no scores, no comment counts."""
    return {
        "id": f"t3_{post['id']}",
        "subreddit": post.get("subreddit", ""),
        "title": post.get("title", ""),
        "author": post.get("author", ""),
        "published_at": iso_z(datetime.fromtimestamp(post["created_utc"], tz=timezone.utc))
                        if post.get("created_utc") else "",
        "url": post.get("url", ""),
        "body": post.get("body", ""),
        "fetched_at": fetched_at,
        "source": "reddit_rss",
    }


def merge_feed(existing: list[dict], fresh_lines: list[dict],
               run_at: datetime) -> tuple[list[dict], int, int]:
    """Id-union merge + 24h retention. Returns (merged, new_count, dup_count).

    new_count  = ids in fresh_lines that were NOT already in the feed
    dup_count  = ids in fresh_lines already present (dropped, never re-written)
    """
    cutoff = run_at - timedelta(hours=WINDOW_HOURS)
    by_id: dict[str, dict] = {}
    order: list[str] = []
    for line in existing:
        pid = line.get("id")
        if not pid:
            continue
        if pid in by_id:
            continue
        by_id[pid] = line
        order.append(pid)
    new_count = 0
    dup_count = 0
    for line in fresh_lines:
        pid = line.get("id")
        if not pid:
            continue
        if pid in by_id:
            dup_count += 1
            continue
        by_id[pid] = line
        order.append(pid)
        new_count += 1
    merged = []
    for pid in order:
        line = by_id[pid]
        ts = line.get("published_at") or ""
        try:
            published = parse_iso_z(ts) if ts else None
        except ValueError:
            published = None
        # Retention: keep only posts published within the rolling window.
        # fetched_at is the fallback anchor so a post with a broken timestamp
        # still ages out instead of living forever.
        anchor = published or parse_iso_z(line.get("fetched_at") or iso_z(run_at))
        if anchor >= cutoff:
            merged.append(line)
    # Newest first so downstream readers see the freshest posts at the top.
    merged.sort(key=lambda l: (l.get("published_at") or l.get("fetched_at") or ""), reverse=True)
    return merged, new_count, dup_count


# ─── Health ───────────────────────────────────────────────────────────

def compute_health(last_successful_run: str | None, run_at: datetime,
                   consecutive_full_failures: int, saw_403: bool) -> tuple[str, list[str]]:
    """Health rules (spec F). Returns (status, notes[]).

    COLLECTOR_OK            last success <= 45 min ago
    COLLECTOR_STALE         last success > 45 min ago
    REDDIT_SOURCE_DEGRADED  RSS failing consecutively or 403-blocked
    An empty delta is NOT a failure and never degrades health by itself.
    """
    notes: list[str] = []
    if saw_403:
        notes.append("HTTP 403 observed this run (edge block fingerprint)")
    if consecutive_full_failures >= DEGRADE_AFTER_CONSEC or saw_403:
        notes.append(f"consecutive full failures: {consecutive_full_failures}")
        return HEALTH_DEGRADED, notes
    if last_successful_run:
        try:
            age_min = (run_at - parse_iso_z(last_successful_run)).total_seconds() / 60
        except ValueError:
            age_min = None
        if age_min is not None:
            if age_min <= STALE_AFTER_MIN:
                return HEALTH_OK, notes
            notes.append(f"last successful run {age_min:.0f} min ago (> {STALE_AFTER_MIN})")
            return HEALTH_STALE, notes
        notes.append("unparseable last_successful_run")
    else:
        notes.append("no successful run recorded yet")
    return HEALTH_STALE, notes


# ─── One collection cycle ─────────────────────────────────────────────

def fetch_all(subs: list[str], state: dict, run_at: datetime,
              budget: int | None) -> tuple[dict[str, dict], list[dict], dict]:
    """Fetch each sub independently. Returns (per_sub, fresh_lines, http_stats).

    per_sub[sub] = {status, fetched, new_estimate, error}
    fresh_lines  = spec-shaped feed lines for all posts fetched this run
    Any single-sub exception is contained; it never aborts the loop.
    """
    rss.reset_fetch_stats()
    if budget is not None:
        rss.set_request_budget(budget)
    fetched_at = iso_z(run_at)
    per_sub: dict[str, dict] = {}
    fresh_lines: list[dict] = []

    for sub in subs:
        entry: dict = {"status": "ok", "fetched": 0, "error": None}
        try:
            cursor = state["subreddit_cursors"].get(sub)
            if cursor:
                posts = rss.fetch_delta(sub, cursor, max_limit=FETCH_LIMIT)
            else:
                posts = rss.fetch_subreddit_new(sub, limit=FETCH_LIMIT)
            if posts is None:
                posts = []
            entry["fetched"] = len(posts)
            # Cursor advances only on success, and only for this sub. The /new
            # feed is newest-first, so posts[0] is the newest seen — same
            # convention as upstream's update_cursor(posts[0]["id"]).
            if posts:
                state["subreddit_cursors"][sub] = posts[0]["id"]
            for p in posts:
                fresh_lines.append(to_feed_line(p, fetched_at))
        except Exception as e:  # isolation: one bad sub must not kill the run
            entry["status"] = "failed"
            entry["error"] = f"{type(e).__name__}: {e}"
        per_sub[sub] = entry

    stats = rss.get_fetch_stats()
    return per_sub, fresh_lines, stats


def run(subs: list[str], budget: int | None = None) -> dict:
    run_at = now_utc()
    state = load_state()
    state["last_run_at"] = iso_z(run_at)

    existing = load_feed()
    per_sub, fresh_lines, stats = fetch_all(subs, state, run_at, budget)

    ok_subs = [s for s, e in per_sub.items() if e["status"] == "ok"]
    failed_subs = [s for s, e in per_sub.items() if e["status"] != "ok"]
    any_ok = bool(ok_subs)
    # A 403 on the keyless RSS surface is a deterministic edge fingerprint
    # block, not a transient: if every sub failed AND the fetch layer saw
    # HTTP 403, the source is degraded immediately rather than waiting out
    # the consecutive-failure counter.
    saw_403 = (not any_ok) and stats.get("http_403", 0) > 0

    summary: dict = {
        "run_at": iso_z(run_at),
        "subs": per_sub,
        "reddit_stats": stats,
        "subs_ok": len(ok_subs),
        "subs_failed": len(failed_subs),
    }

    if any_ok:
        # Success path: rebuild the feed (id-union + 24h window), then persist.
        merged, new_count, dup_count = merge_feed(existing, fresh_lines, run_at)
        feed_text = "".join(json.dumps(line, ensure_ascii=False) + "\n" for line in merged)
        atomic_write(FEED_PATH, feed_text)

        state["last_successful_run"] = iso_z(run_at)
        state["consecutive_full_failures"] = 0
        state["seen_count"] = len(merged)
        state["total_seen_count"] = state.get("total_seen_count", 0) + new_count
        state["last_run_result"] = "ok"
        atomic_write(STATE_PATH, json.dumps(state, indent=2, ensure_ascii=False) + "\n")

        status, notes = compute_health(state["last_successful_run"], now_utc(),
                                       state["consecutive_full_failures"], saw_403=False)
        summary.update({"new_posts": new_count, "duplicates_skipped": dup_count,
                        "feed_lines": len(merged)})
    else:
        # Full failure: DO NOT touch the feed. Record the failure, keep cursors.
        state["consecutive_full_failures"] = state.get("consecutive_full_failures", 0) + 1
        state["last_run_result"] = "failed"
        atomic_write(STATE_PATH, json.dumps(state, indent=2, ensure_ascii=False) + "\n")
        status, notes = compute_health(state.get("last_successful_run"), now_utc(),
                                       state["consecutive_full_failures"], saw_403=saw_403)
        summary.update({"new_posts": 0, "duplicates_skipped": 0,
                        "feed_lines": len(existing),
                        "feed_preserved": True})

    health = {
        "status": status,
        "generated_at": iso_z(now_utc()),
        "run_at": iso_z(run_at),
        "last_successful_run": state.get("last_successful_run"),
        "consecutive_full_failures": state.get("consecutive_full_failures", 0),
        "empty_feed_is_not_no_opportunity": True,
        "notes": notes,
        "last_run": summary,
    }
    atomic_write(HEALTH_PATH, json.dumps(health, indent=2, ensure_ascii=False) + "\n")
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reddit RSS collector cycle")
    parser.add_argument("--subs", type=str, default=None,
                        help="comma-separated subreddit override")
    parser.add_argument("--only-sub", type=str, default=None, dest="only_sub",
                        help="collect a SINGLE subreddit and commit incrementally "
                             "(operational/backfill knob; the scheduled path runs "
                             "the full sub list). Same fetch/merge/commit code path.")
    parser.add_argument("--budget", type=int, default=None,
                        help="max Reddit GETs this run (default unlimited)")
    args = parser.parse_args(argv)

    if args.only_sub:
        subs = [args.only_sub.strip()]
    elif args.subs:
        subs = [s.strip() for s in args.subs.split(",") if s.strip()]
    else:
        subs = DEFAULT_SUBS
    summary = run(subs, budget=args.budget)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    # Exit 0 even when some subs failed: the artifacts (health.json) carry the
    # state, and a nonzero exit in CI would just obscure the health semantics.
    return 0


if __name__ == "__main__":
    sys.exit(main())
