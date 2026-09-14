"""Offline unit tests for the bridge collector (no network, no Reddit calls).

Covers the gate's reliability requirements:
  - Atom parsing correctness (entities, chrome, footer)
  - per-sub cursor delta cut
  - 429 retry/backoff behavior
  - 403 dual-host failover
  - feed merge: id-union dedup, kill-safety (crash between feed and state
    writes cannot pollute data), 24h retention
  - health state machine (OK / STALE / DEGRADED)
  - per-sub failure isolation
  - feed line shape (exact spec keys, no fabricated fields)
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "collector"))

import reddit as rss          # noqa: E402
import run as collector       # noqa: E402

UTC = timezone.utc


def iso_z(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_post(pid: str, sub: str, published: datetime, title: str = "t") -> dict:
    return {"id": pid, "subreddit": sub, "title": title, "url": f"https://www.reddit.com/r/{sub}/comments/{pid}/x/",
            "canonical_url": f"https://reddit.com/comments/{pid}/", "author": "someone",
            "created_utc": int(published.timestamp()), "score": 0, "num_comments": 0,
            "body": "body text", "upvote_ratio": None, "removed": False, "locked": False,
            "over_18": False, "is_crosspost": False}


ATOM_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <category term="SaaS" label="r/SaaS"/>
  <entry>
    <author><name>/u/TestUser</name><uri>https://www.reddit.com/user/TestUser</uri></author>
    <category term="SaaS" label="r/SaaS"/>
    <content type="html">&lt;!-- SC_OFF --&gt;&lt;div class="md"&gt;&lt;p&gt;Hello &amp;amp; welcome&lt;/p&gt;&lt;p&gt;Second line&lt;/p&gt;&lt;/div&gt;&lt;!-- SC_ON --&gt; &amp;#32; submitted by &amp;#32; &lt;a href="https://www.reddit.com/user/TestUser"&gt; /u/TestUser &lt;/a&gt; &amp;#32;[link]&amp;#32; &amp;#32; &lt;span&gt;&lt;a href="x"&gt;[comments]&lt;/a&gt;&lt;/span&gt;</content>
    <id>t3_abc123</id>
    <link href="https://www.reddit.com/r/SaaS/comments/abc123/hello/"/>
    <updated>2026-09-13T10:00:00+00:00</updated>
    <published>2026-09-13T09:59:00+00:00</published>
    <title>Hello &amp; welcome</title>
  </entry>
</feed>"""


class FakeResp:
    def __init__(self, body: bytes, headers: dict | None = None):
        self._body = body
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class AtomParsingTest(unittest.TestCase):
    def test_parse_atom_entry(self):
        root = rss.ET.fromstring(ATOM_FEED)
        entries = root.findall(f"{rss._ATOM_NS}entry")
        self.assertEqual(len(entries), 1)
        p = rss.parse_atom_entry(entries[0])
        self.assertIsNotNone(p)
        self.assertEqual(p["id"], "abc123")
        self.assertEqual(p["subreddit"], "SaaS")
        self.assertEqual(p["author"], "TestUser")
        self.assertEqual(p["title"], "Hello & welcome")
        self.assertEqual(p["created_utc"],
                         int(datetime(2026, 9, 13, 9, 59, tzinfo=UTC).timestamp()))
        self.assertIn("Hello & welcome", p["body"])
        self.assertIn("Second line", p["body"])
        self.assertNotIn("submitted by", p["body"])
        self.assertNotIn("SC_OFF", p["body"])

    def test_parse_rejects_comment_entry(self):
        xml = ('<entry xmlns="http://www.w3.org/2005/Atom">'
               '<id>t1_p9irw4t</id><link href="https://www.reddit.com/r/SaaS/comments/abc123/x/p9irw4t/"/>'
               '<title>a comment</title></entry>')
        entry = rss.ET.fromstring(xml)
        # t1_ id fails the t3/post guard; permalink fallback also yields the
        # post id only when pattern matches the comment link's parent — the
        # upstream guard rejects ids that are not bare t3-style.
        self.assertIsNone(rss.parse_atom_entry(entry))

    def test_delta_from_posts(self):
        posts = [make_post("a3", "SaaS", datetime(2026, 9, 13, 10, tzinfo=UTC)),
                 make_post("a2", "SaaS", datetime(2026, 9, 13, 9, tzinfo=UTC)),
                 make_post("a1", "SaaS", datetime(2026, 9, 13, 8, tzinfo=UTC))]
        self.assertEqual([p["id"] for p in rss.delta_from_posts(posts, None)], ["a3", "a2", "a1"])
        self.assertEqual([p["id"] for p in rss.delta_from_posts(posts, "a2")], ["a3"])
        self.assertEqual([p["id"] for p in rss.delta_from_posts(posts, "zzz")], ["a3", "a2", "a1"])
        self.assertEqual([p["id"] for p in rss.delta_from_posts(posts, None, max_limit=2)], ["a3", "a2"])


class BackoffTest(unittest.TestCase):
    def setUp(self):
        rss.reset_fetch_stats()

    def test_429_retry_then_success(self):
        calls = {"n": 0}
        sleeps: list[float] = []

        def fake_urlopen(req, timeout=None, context=None):
            calls["n"] += 1
            if calls["n"] <= 2:
                hdrs = {"Retry-After": "5", "x-ratelimit-remaining": "0"}
                raise rss.urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", hdrs, None)
            return FakeResp(ATOM_FEED.encode(), {"x-ratelimit-remaining": "10"})

        with mock.patch.object(rss.urllib.request, "urlopen", side_effect=fake_urlopen), \
             mock.patch.object(rss, "_sleep", lambda s: sleeps.append(s)):
            status, root = rss._fetch_xml_attempt("https://www.reddit.com/r/SaaS/new/.rss")
        self.assertEqual(status, "ok")
        self.assertEqual(calls["n"], 3)
        # sleeps include the 1.6s inter-request throttle; the 429 backoffs are
        # the large entries and must equal the Retry-After value twice.
        backoffs = [s for s in sleeps if s > rss.MIN_REQUEST_INTERVAL + 0.5]
        self.assertEqual(backoffs, [5.0, 5.0])
        self.assertIsNotNone(root)

    def test_429_exhausted(self):
        sleeps: list[float] = []

        def fake_urlopen(req, timeout=None, context=None):
            hdrs = {"x-ratelimit-reset": "30"}
            raise rss.urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", hdrs, None)

        with mock.patch.object(rss.urllib.request, "urlopen", side_effect=fake_urlopen), \
             mock.patch.object(rss, "_sleep", lambda s: sleeps.append(s)):
            status, root = rss._fetch_xml_attempt("https://www.reddit.com/r/SaaS/new/.rss")
        self.assertEqual(status, "rate_limited")
        self.assertEqual(root, None)
        # MAX_RETRIES=3 attempts -> 2 backoff sleeps (plus 1.6s throttle
        # spacing). Retry-After absent -> x-ratelimit-reset (30s) wins,
        # capped at MAX_RATELIMIT_PAUSE.
        backoffs = [s for s in sleeps if s > rss.MIN_REQUEST_INTERVAL + 0.5]
        self.assertEqual(backoffs, [30.0, 30.0])
        self.assertTrue(rss.is_rate_limited())
        self.assertEqual(rss.get_fetch_stats()["rate_limited"], 0)  # counter is the caller's job

    def test_403_failover_to_old_reddit(self):
        calls = {"hosts": []}

        def fake_urlopen(req, timeout=None, context=None):
            calls["hosts"].append(req.full_url.split("/")[2])
            if len(calls["hosts"]) == 1:
                raise rss.urllib.error.HTTPError(req.full_url, 403, "Blocked", {}, None)
            return FakeResp(ATOM_FEED.encode(), {})

        with mock.patch.object(rss.urllib.request, "urlopen", side_effect=fake_urlopen), \
             mock.patch.object(rss, "_sleep", lambda s: None):
            root = rss.fetch_xml_resilient("/r/SaaS/new/.rss")
        self.assertIsNotNone(root)
        self.assertEqual(calls["hosts"], ["www.reddit.com", "old.reddit.com"])
        stats = rss.get_fetch_stats()
        self.assertEqual(stats["ok"], 1)
        self.assertEqual(stats["fallback_used"], 1)
        self.assertEqual(stats["http_403"], 1)

    def test_403_on_all_hosts(self):
        def fake_urlopen(req, timeout=None, context=None):
            raise rss.urllib.error.HTTPError(req.full_url, 403, "Blocked", {}, None)

        with mock.patch.object(rss.urllib.request, "urlopen", side_effect=fake_urlopen), \
             mock.patch.object(rss, "_sleep", lambda s: None):
            root = rss.fetch_xml_resilient("/r/SaaS/new/.rss")
        self.assertIsNone(root)
        stats = rss.get_fetch_stats()
        self.assertEqual(stats["failed"], 1)
        self.assertEqual(stats["http_403"], 2)

    def test_429_never_fails_over(self):
        """429 must NOT advance to old.reddit: the bucket is shared per IP."""
        calls = {"n": 0}

        def fake_urlopen(req, timeout=None, context=None):
            calls["n"] += 1
            raise rss.urllib.error.HTTPError(req.full_url, 429, "Slow down", {}, None)

        with mock.patch.object(rss.urllib.request, "urlopen", side_effect=fake_urlopen), \
             mock.patch.object(rss, "_sleep", lambda s: None):
            root = rss.fetch_xml_resilient("/r/SaaS/new/.rss")
        self.assertIsNone(root)
        self.assertEqual(calls["n"], 3)  # one host only, MAX_RETRIES=3


class FeedMergeTest(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
        self.old_ts = iso_z(self.now - timedelta(hours=30))
        self.fresh_ts = iso_z(self.now - timedelta(hours=2))
        self.existing = [
            {"id": "t3_old1", "subreddit": "SaaS", "title": "old1", "author": "a",
             "published_at": self.old_ts, "url": "u", "body": "", "fetched_at": self.old_ts,
             "source": "reddit_rss"},
            {"id": "t3_keep1", "subreddit": "SaaS", "title": "keep1", "author": "a",
             "published_at": self.fresh_ts, "url": "u", "body": "", "fetched_at": self.fresh_ts,
             "source": "reddit_rss"},
        ]
        self.fresh = [
            {"id": "t3_keep1", "subreddit": "SaaS", "title": "keep1", "author": "a",
             "published_at": self.fresh_ts, "url": "u", "body": "", "fetched_at": self.fresh_ts,
             "source": "reddit_rss"},
            {"id": "t3_new1", "subreddit": "SaaS", "title": "new1", "author": "a",
             "published_at": self.fresh_ts, "url": "u", "body": "", "fetched_at": self.fresh_ts,
             "source": "reddit_rss"},
        ]

    def test_union_dedup_and_retention(self):
        merged, new_count, dup_count = collector.merge_feed(self.existing, self.fresh, self.now)
        ids = [line["id"] for line in merged]
        self.assertEqual(len(ids), len(set(ids)), "duplicate ids must never be written")
        self.assertEqual(new_count, 1)
        self.assertEqual(dup_count, 1)
        self.assertIn("t3_new1", ids)
        self.assertIn("t3_keep1", ids)
        self.assertNotIn("t3_old1", ids, "30h-old post must age out of the 24h window")
        # newest first
        self.assertEqual(merged[0]["published_at"] >= merged[-1]["published_at"], True)

    def test_kill_safety_double_merge(self):
        """Crash between feed write and state write: rerun refetches the same
        posts; the id-union must absorb them with zero pollution."""
        merged1, new1, dup1 = collector.merge_feed(self.existing, self.fresh, self.now)
        self.assertEqual(new1, 1)
        # Simulate the crash: feed persisted, cursor/state NOT advanced.
        # Next run refetches the same posts.
        merged2, new2, dup2 = collector.merge_feed(merged1, self.fresh, self.now)
        self.assertEqual(new2, 0)
        self.assertEqual(dup2, 2)
        ids = [line["id"] for line in merged2]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len(merged2), len(merged1), "rerun must not grow the feed")


class HealthTest(unittest.TestCase):
    def test_ok_within_45min(self):
        now = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
        status, _ = collector.compute_health(iso_z(now - timedelta(minutes=10)), now, 0, False)
        self.assertEqual(status, collector.HEALTH_OK)

    def test_stale_after_45min(self):
        now = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
        status, _ = collector.compute_health(iso_z(now - timedelta(minutes=50)), now, 0, False)
        self.assertEqual(status, collector.HEALTH_STALE)

    def test_never_succeeded_is_stale(self):
        now = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
        status, _ = collector.compute_health(None, now, 0, False)
        self.assertEqual(status, collector.HEALTH_STALE)

    def test_consecutive_failures_degrade(self):
        now = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
        status, _ = collector.compute_health(iso_z(now - timedelta(minutes=5)), now, 2, False)
        self.assertEqual(status, collector.HEALTH_DEGRADED)

    def test_403_degrades_immediately(self):
        now = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)
        status, notes = collector.compute_health(iso_z(now - timedelta(minutes=1)), now, 0, True)
        self.assertEqual(status, collector.HEALTH_DEGRADED)
        self.assertTrue(any("403" in n for n in notes))


class FeedLineShapeTest(unittest.TestCase):
    def test_exact_keys(self):
        post = make_post("abc", "SaaS", datetime(2026, 9, 13, 10, tzinfo=UTC))
        line = collector.to_feed_line(post, "2026-09-13T12:00:00Z")
        self.assertEqual(tuple(sorted(line.keys())),
                         tuple(sorted(collector.REQUIRED_LINE_KEYS)))
        self.assertEqual(line["id"], "t3_abc")
        self.assertEqual(line["source"], "reddit_rss")
        self.assertEqual(line["published_at"], "2026-09-13T10:00:00Z")
        # No fabricated engagement metrics anywhere in the line
        self.assertNotIn("score", line)
        self.assertNotIn("num_comments", line)
        self.assertNotIn("upvote_ratio", line)


class FailureIsolationTest(unittest.TestCase):
    def test_one_sub_failure_does_not_kill_others(self):
        state = {"subreddit_cursors": {}}

        def fake_fetch(sub, limit=25, timeout=15):
            if sub == "broken":
                raise RuntimeError("simulated sub failure")
            return [make_post(f"{sub}1", sub, datetime(2026, 9, 13, 10, tzinfo=UTC))]

        with mock.patch.object(collector.rss, "fetch_subreddit_new", side_effect=fake_fetch):
            per_sub, fresh_lines, stats = collector.fetch_all(
                ["broken", "SaaS"], state, datetime.now(UTC), budget=None)
        self.assertEqual(per_sub["broken"]["status"], "failed")
        self.assertIn("simulated", per_sub["broken"]["error"])
        self.assertEqual(per_sub["SaaS"]["status"], "ok")
        self.assertEqual(len(fresh_lines), 1)
        self.assertEqual(state["subreddit_cursors"].get("SaaS"), "SaaS1")
        self.assertNotIn("broken", state["subreddit_cursors"])

    def test_full_failure_reports_stats(self):
        state = {"subreddit_cursors": {}}

        def fake_fetch(sub, limit=25, timeout=15):
            return []  # unreachable -> fetch layer returns []

        with mock.patch.object(collector.rss, "fetch_subreddit_new", side_effect=fake_fetch), \
             mock.patch.object(collector.rss, "get_fetch_stats",
                               return_value={"ok": 0, "failed": 2, "rate_limited": 0,
                                             "fallback_used": 0, "http_403": 2}):
            per_sub, fresh_lines, stats = collector.fetch_all(
                ["SaaS", "shopify"], state, datetime.now(UTC), budget=None)
        self.assertEqual(stats["http_403"], 2)
        self.assertEqual(fresh_lines, [])


class AtomicWriteTest(unittest.TestCase):
    def test_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "x.json"
            collector.atomic_write(p, '{"a": 1}\n')
            self.assertEqual(p.read_text(), '{"a": 1}\n')
            collector.atomic_write(p, '{"a": 2}\n')
            self.assertEqual(p.read_text(), '{"a": 2}\n')
            leftovers = [f for f in Path(td).iterdir() if f.name.startswith(".tmp-")]
            self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
