#!/usr/bin/env python3
"""Daily Morning Briefing data collector (version 2).

Runs on GitHub Actions (or any machine with internet). Standard library only.
Reads config.json, fetches public RSS/Atom feeds that publishers offer plus
public sports score endpoints, probes whether one article per outlet can be
fetched, and writes:

  digest_status.md   HEALTH flag, what worked, every failure with details (read this first)
  digest_brief.md    condensed headlines grouped by story, multi-outlet stories first (read this one)
  digest_news.md     recent headlines by source (title, time, link, short snippet)
  digest_sports.md   finals, live games and the next 7 days by league, my teams flagged
  digest_teams.md    per-team news headlines and injury lists for my teams (ESPN endpoints)
  digest.json        everything above in structured form
  failure_log.md     rolling log of failures per run (last 45 runs)

There is no cap on the number of feeds: every entry in config.json "news_feeds" is read.
Nothing here reads anyone's email or logs in anywhere. Failures are recorded, never hidden.
Article text from the probe is measured and discarded; it is never stored.
"""
import html
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser
from zoneinfo import ZoneInfo

COLLECTOR_VERSION = 8
ET_TZ = ZoneInfo("America/New_York")
UA = "MorningBriefingCollector/2.0 (personal use; one run per day)"
NOW = datetime.now(timezone.utc)
NEWS_WINDOW_HOURS = 48
MD_ITEMS_PER_FEED = 15
JSON_ITEMS_PER_FEED = 40
OTHERS_PER_LEAGUE = 25
NEWS_BUDGET_SECONDS = 600
PROBE_BUDGET_SECONDS = 300
FINAL_WINDOW_HOURS = 40
LOOKAHEAD_DAYS = 7
LOG_KEEP_RUNS = 45

FAILURES = []  # every failure/warning of this run, as dicts


# ---------- errors and failure records ----------
class FetchError(Exception):
    def __init__(self, kind, detail, http_code=None, attempts=1):
        super().__init__(f"{kind}: {detail}")
        self.kind, self.detail, self.http_code, self.attempts = kind, detail, http_code, attempts


def likely_cause(kind, code):
    """Every sentence here is a judgment (INFERRED); the error text itself is the verified part."""
    if kind == "HTTP_ERROR":
        if code in (401, 403):
            return "INFERRED: access refused (bot protection, an IP block, or a login/paywall). The response reports only the status code."
        if code in (404, 410):
            return "INFERRED: the URL no longer exists or has moved."
        if code == 429:
            return "INFERRED: the server rate-limited the request."
        if code and code >= 500:
            return "INFERRED: a problem on the server side."
        return "INFERRED: unclear from the status code alone."
    return {
        "TIMEOUT": "INFERRED: the server was slow or unreachable from this runner.",
        "NETWORK_ERROR": "INFERRED: a DNS or connection problem between the runner and the server.",
        "SSL_ERROR": "INFERRED: a certificate or TLS problem with the server.",
        "PARSE_ERROR": "INFERRED: the response was probably a web page (a block or error page), not a feed or valid data.",
        "EMPTY_FEED": "INFERRED: the feed format changed, or the feed is empty.",
        "STALE": "INFERRED: the publisher stopped updating this feed, or the feed is cached (it may be a frozen archive).",
        "UNDATED": "INFERRED: the feed carries no publication dates, so freshness cannot be checked (it may be a frozen archive).",
        "EMPTY_RESPONSE": "INFERRED: the server (or its bot protection) answered with nothing instead of the feed. The response was an empty body, not a web page.",
        "THIN": "INFERRED: the page is probably rendered by JavaScript, or sits behind a paywall or notice.",
        "ROBOTS_DISALLOWED": "VERIFIED by the site's robots.txt: the site asks automated tools not to fetch this page, so the collector did not.",
        "TIME_BUDGET": "VERIFIED by the collector's own clock: the time limit set in config.json (limits) ran out. Raise it or trim the feed list.",
        "ROBOTS_UNREADABLE": "INFERRED: the site's robots.txt could not be read (server error or network problem), so the collector skipped the page to be safe (RFC 9309 rule for server errors).",
        "CONFIG_ERROR": "VERIFIED by the error text: config.json is not valid or is missing a field.",
    }.get(kind, "INFERRED: unknown; see the error text.")


def describe(e):
    """Return (kind, http_code, detail, attempts) for any exception."""
    if isinstance(e, FetchError):
        return e.kind, e.http_code, e.detail, e.attempts
    if isinstance(e, ET.ParseError):
        return "PARSE_ERROR", None, f"invalid XML: {e}", 1
    if isinstance(e, json.JSONDecodeError):
        return "PARSE_ERROR", None, f"invalid JSON: {e}", 1
    return "UNEXPECTED", None, f"{type(e).__name__}: {e}", 1


def log_failure(step, source, url, kind, code, detail, attempts, impact, severity="FAIL"):
    FAILURES.append({
        "time_et": et_str(datetime.now(timezone.utc)), "severity": severity, "step": step, "source": source,
        "url": url, "error_kind": kind, "http_code": code, "error": str(detail)[:300], "attempts": attempts,
        "impact": impact, "likely_cause": likely_cause(kind, code),
    })


def log_exception(step, source, url, e, impact, severity="FAIL"):
    kind, code, detail, attempts = describe(e)
    log_failure(step, source, url, kind, code, detail, attempts, impact, severity)


def format_record(r):
    code = f" {r['http_code']}" if r.get("http_code") else ""
    return (f"- {r['time_et']} | {r['severity']} | step: {r['step']} | {r['source']} | {r['url']} | "
            f"{r['error_kind']}{code}: {r['error']} | attempts: {r['attempts']} | impact: {r['impact']} | "
            f"likely cause: {r['likely_cause']}")


# ---------- helpers ----------
def fetch(url, timeout=25, tries=2):
    last = None
    for i in range(1, tries + 1):
        try:
            req = Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
            with urlopen(req, timeout=timeout) as r:
                return r.read()
        except HTTPError as e:
            last = FetchError("HTTP_ERROR", f"{e.code} {e.reason}", e.code, i)
            if e.code in (401, 403, 404, 410):
                break  # retrying will not help
        except URLError as e:
            reason = str(e.reason)
            low = reason.lower()
            if "timed out" in low or isinstance(e.reason, TimeoutError):
                kind = "TIMEOUT"
            elif "ssl" in low or "certificate" in low:
                kind = "SSL_ERROR"
            else:
                kind = "NETWORK_ERROR"
            last = FetchError(kind, reason, None, i)
        except TimeoutError as e:
            last = FetchError("TIMEOUT", str(e) or "timed out", None, i)
        except OSError as e:
            last = FetchError("NETWORK_ERROR", f"{type(e).__name__}: {e}", None, i)
        if i < tries:
            time.sleep(1.5)
    raise last


def parse_iso(s):
    if not s:
        return None
    try:
        s = s.strip().replace("Z", "+00:00")
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except ValueError:
        return None


def parse_rfc(s):
    if not s:
        return None
    try:
        d = parsedate_to_datetime(s.strip())
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return parse_iso(s)


def et_str(d):
    if d is None:
        return "undated"
    e = d.astimezone(ET_TZ)
    return e.strftime("%a %b ") + str(e.day) + e.strftime(", ") + str(int(e.strftime("%I"))) + e.strftime(":%M %p ET")


def clean(text, limit=220):
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", text)
    t = html.unescape(t)
    t = re.sub(r"\s+", " ", t).strip()
    return t if len(t) <= limit else t[: limit - 1].rstrip() + "…"


def local(tag):
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def child_text(el, name):
    for c in el:
        if local(c.tag) == name:
            return (c.text or "").strip()
    return ""


# ---------- robots.txt ----------
ROBOTS = {}  # origin -> (parser or None, note)


def robots_allows(url):
    """Follow RFC 9309. robots.txt readable: obey it. Other 4xx (including 403/404): no restrictions.
    5xx or network error: treat as disallowed and skip. Returns (allowed, note)."""
    parts = urlparse(url)
    origin = f"{parts.scheme}://{parts.netloc}"
    if origin not in ROBOTS:
        rp = RobotFileParser()
        try:
            rp.parse(fetch(origin + "/robots.txt", timeout=15, tries=1).decode("utf-8", "ignore").splitlines())
            ROBOTS[origin] = (rp, "robots.txt read")
        except FetchError as e:
            if e.http_code and 400 <= e.http_code < 500:
                rp.parse([])
                ROBOTS[origin] = (rp, f"robots.txt unavailable (HTTP {e.http_code}); treated as no restrictions per RFC 9309")
            else:
                ROBOTS[origin] = (None, f"robots.txt could not be read ({e.kind}: {e.detail})")
    rp, note = ROBOTS[origin]
    if rp is None:
        return False, note
    return rp.can_fetch(UA, url), note


# ---------- news feeds ----------
def parse_feed(data):
    if not data.strip():
        raise FetchError("EMPTY_RESPONSE", "HTTP request succeeded but the response body was empty")
    head = data[:400].lstrip().lower()
    if head.startswith(b"<!doctype html") or head.startswith(b"<html"):
        raise FetchError("PARSE_ERROR", "response is an HTML web page, not a feed")
    root = ET.fromstring(data)
    items = []
    for el in root.iter():
        n = local(el.tag)
        if n == "item":  # RSS
            link = child_text(el, "link")
            pub = parse_rfc(child_text(el, "pubDate") or child_text(el, "date"))
            desc = child_text(el, "description")
            items.append({"title": clean(child_text(el, "title"), 300), "link": link, "published": pub, "snippet": clean(desc)})
        elif n == "entry":  # Atom
            link = ""
            for c in el:
                if local(c.tag) == "link" and c.attrib.get("href"):
                    if c.attrib.get("rel", "alternate") == "alternate" or not link:
                        link = c.attrib["href"]
            pub = parse_iso(child_text(el, "published") or child_text(el, "updated"))
            desc = child_text(el, "summary") or child_text(el, "content")
            items.append({"title": clean(child_text(el, "title"), 300), "link": link, "published": pub, "snippet": clean(desc)})
    return items


def collect_news(cfg, status):
    out = []
    cutoff = NOW - timedelta(hours=NEWS_WINDOW_HOURS)
    t0 = time.time()
    for feed in cfg.get("news_feeds", []):
        try:
            name, url = feed["name"], feed["url"]
        except (KeyError, TypeError):
            log_failure("Feed sweep", str(feed)[:60], "config.json", "CONFIG_ERROR", None,
                        "a news_feeds entry is missing \"name\" or \"url\"", 0, "this feed entry was skipped")
            continue
        if time.time() - t0 > NEWS_BUDGET_SECONDS:
            status["feeds"].append({"name": name, "ok": False, "error": "TIME_BUDGET: not attempted", "url": url})
            log_failure("Feed sweep", name, url, "TIME_BUDGET", None,
                        f"not attempted: the {NEWS_BUDGET_SECONDS}-second feed time budget was used up", 0, "no items from this outlet in the digest")
            continue
        try:
            allowed, rnote = robots_allows(url)
            if not allowed:
                if rnote.startswith("robots.txt could not be read"):
                    raise FetchError("ROBOTS_UNREADABLE", rnote + "; feed not requested")
                raise FetchError("ROBOTS_DISALLOWED", "robots.txt disallows this feed URL; feed not requested")
            items = parse_feed(fetch(url))
            if not items:
                raise FetchError("EMPTY_FEED", "feed parsed but contained no items")
            dated = [i for i in items if i["published"]]
            if not dated:
                raise FetchError("UNDATED", f"none of the {len(items)} items carries a publication date, so freshness cannot be verified; feed excluded")
            newest = max(i["published"] for i in dated)
            if newest < cutoff:
                raise FetchError("STALE", f"newest dated item is {et_str(newest)}, older than {NEWS_WINDOW_HOURS} hours; feed excluded")
            keep = [i for i in dated if i["published"] >= cutoff]  # undated items are dropped: freshness cannot be verified
            keep.sort(key=lambda i: i["published"], reverse=True)
            status["feeds"].append({"name": name, "ok": True, "items_in_window": len(keep), "newest": et_str(newest), "url": url})
            out.append({"source": name, "url": url, "category": feed.get("category", ""), "items": keep[:JSON_ITEMS_PER_FEED]})
        except Exception as e:  # keep going; record the failure
            kind, code, detail, attempts = describe(e)
            status["feeds"].append({"name": name, "ok": False, "error": f"{kind} {code or ''}: {detail}"[:200], "url": url})
            log_failure("Feed sweep", name, url, kind, code, detail, attempts, "no items from this outlet in the digest")
    return out


# ---------- article access probe ----------
def visible_text_chars(data):
    t = data.decode("utf-8", "ignore")
    t = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = html.unescape(t)
    return len(re.sub(r"\s+", " ", t).strip())


def collect_probe(cfg, status, news):
    """Try ONE article per outlet host. Honors robots.txt. Measures the page and discards it."""
    pcfg = cfg.get("probe", {})
    if not pcfg.get("enabled", True):
        return
    min_chars = pcfg.get("min_text_chars", 1500)
    seen = set()
    tstart = time.time()
    for feed in news:
        if time.time() - tstart > PROBE_BUDGET_SECONDS:
            log_failure("Probe", "remaining outlets", "n/a", "TIME_BUDGET", None,
                        f"the {PROBE_BUDGET_SECONDS}-second probe time budget was used up; remaining outlets were not probed", 0,
                        "article access is unknown for the outlets not probed", "WARN")
            break
        item = next((i for i in feed["items"] if i.get("link")), None)
        if not item:
            continue
        url = item["link"]
        parts = urlparse(url)
        host = parts.netloc
        if not host or host in seen:
            continue
        seen.add(host)
        rec = {"source": feed["source"], "host": host, "url": url}
        t0 = time.time()
        try:
            allowed, rnote = robots_allows(url)
            if not allowed:
                unreadable = rnote.startswith("robots.txt could not be read")
                kind = "ROBOTS_UNREADABLE" if unreadable else "ROBOTS_DISALLOWED"
                rec.update(result="SKIPPED_ROBOTS", note=rnote if unreadable else "robots.txt disallows this page; it was not requested")
                log_failure("Probe", feed["source"], url, kind, None, rnote if unreadable else "robots.txt disallows fetching this page", 0,
                            "article detail from this outlet is not available to the collector", "WARN")
            else:
                data = fetch(url, timeout=15, tries=1)
                chars = visible_text_chars(data)
                rec.update(result="OK" if chars >= min_chars else "THIN", bytes=len(data), text_chars=chars)
                if chars < min_chars:
                    log_failure("Probe", feed["source"], url, "THIN", None,
                                f"page loaded but has only {chars} characters of visible text (threshold {min_chars})", 1,
                                "article detail from this outlet is probably not readable by the collector", "WARN")
        except Exception as e:
            kind, code, detail, attempts = describe(e)
            rec.update(result="FAILED", error_kind=kind, http_code=code, error=detail[:200])
            log_failure("Probe", feed["source"], url, kind, code, detail, attempts,
                        "article detail from this outlet could not be fetched by the collector", "WARN")
        rec["seconds"] = round(time.time() - t0, 1)
        status["probe"].append(rec)
        time.sleep(1)


# ---------- sports ----------
def is_my_team(label, my_teams):
    low = (label or "").lower()
    return any(t.lower() in low for t in my_teams)


def espn_events(sport, league, groups, start, end):
    base = f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard"
    q = f"?dates={start:%Y%m%d}-{end:%Y%m%d}&limit=400"
    if groups:
        q += f"&groups={groups}"
    try:
        return json.loads(fetch(base + q)).get("events", [])
    except Exception:
        events, seen = [], set()
        d = start
        while d <= end:  # fall back to one call per day
            qq = f"?dates={d:%Y%m%d}&limit=400" + (f"&groups={groups}" if groups else "")
            for ev in json.loads(fetch(base + qq)).get("events", []):
                if ev.get("id") not in seen:
                    seen.add(ev.get("id"))
                    events.append(ev)
            d += timedelta(days=1)
        return events


def shape_espn_event(ev, sport, my_teams):
    comp = (ev.get("competitions") or [{}])[0]
    st = (ev.get("status") or comp.get("status") or {}).get("type", {})
    start = parse_iso(ev.get("date"))
    rows = []
    for c in comp.get("competitors", []):
        label = (c.get("team") or {}).get("displayName") or (c.get("athlete") or {}).get("displayName") or ""
        rank = (c.get("curatedRank") or {}).get("current")
        rows.append({
            "name": label, "homeAway": c.get("homeAway"), "score": c.get("score"),
            "winner": c.get("winner"), "rank": rank if rank and rank < 99 else None,
            "order": c.get("order"),
        })
    if sport == "golf":
        rows = sorted(rows, key=lambda r: (r.get("order") is None, r.get("order") or 0))[:10]
    broadcasts = []
    for b in comp.get("broadcasts", []) or []:
        broadcasts += b.get("names", [])
    names = " ".join(r["name"] for r in rows) + " " + ev.get("name", "")
    return {
        "id": ev.get("id"), "name": ev.get("name"), "start_utc": start.isoformat() if start else None,
        "start_et": et_str(start), "state": st.get("state"), "status": st.get("shortDetail") or st.get("description"),
        "completed": st.get("completed"), "venue": (comp.get("venue") or {}).get("fullName"),
        "broadcast": ", ".join(dict.fromkeys(broadcasts)) or None, "competitors": rows,
        "my_team": is_my_team(names, my_teams), "_start": start,
    }


def collect_espn(cfg, status):
    leagues = {}
    start = (NOW - timedelta(days=2)).date()
    end = (NOW + timedelta(days=LOOKAHEAD_DAYS)).date()
    my_teams = cfg.get("my_teams", [])
    for lg in cfg.get("espn_leagues", []):
        key = f"{lg['sport']}/{lg['league']}"
        url = f"https://site.api.espn.com/apis/site/v2/sports/{key}/scoreboard"
        try:
            raw = espn_events(lg["sport"], lg["league"], lg.get("groups"), start, end)
            events = [shape_espn_event(e, lg["sport"], my_teams) for e in raw]
            leagues[lg["label"]] = events
            status["sports"].append({"name": lg["label"], "ok": True, "events": len(events), "source": "ESPN scoreboard", "key": key})
        except Exception as e:
            kind, code, detail, attempts = describe(e)
            status["sports"].append({"name": lg["label"], "ok": False, "error": f"{kind} {code or ''}: {detail}"[:200], "source": "ESPN scoreboard", "key": key})
            log_failure("Sports data", lg["label"], url, kind, code, detail, attempts, f"no {lg['label']} scores or schedule in the digest")
    return leagues


def collect_mlb(cfg, status):
    my_teams = cfg.get("my_teams", [])
    start = (NOW - timedelta(days=2)).date()
    end = (NOW + timedelta(days=LOOKAHEAD_DAYS)).date()
    url = ("https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=%s&endDate=%s"
           "&hydrate=team,linescore,seriesStatus" % (start.isoformat(), end.isoformat()))
    try:
        data = json.loads(fetch(url))
        games = []
        for day in data.get("dates", []):
            for g in day.get("games", []):
                away, home = g["teams"]["away"], g["teams"]["home"]
                start_dt = parse_iso(g.get("gameDate"))
                st = g.get("status", {})
                games.append({
                    "id": g.get("gamePk"), "start_utc": start_dt.isoformat() if start_dt else None, "start_et": et_str(start_dt),
                    "state": {"Final": "post", "Live": "in", "Preview": "pre"}.get(st.get("abstractGameState"), st.get("abstractGameState")),
                    "status": st.get("detailedState"), "game_type": g.get("gameType"),
                    "description": g.get("description") or g.get("seriesDescription"),
                    "series_game": g.get("seriesGameNumber"), "games_in_series": g.get("gamesInSeries"),
                    "series_status": (g.get("seriesStatus") or {}).get("result") or (g.get("seriesStatus") or {}).get("shortDescription"),
                    "competitors": [
                        {"name": away["team"]["name"], "homeAway": "away", "score": away.get("score"), "winner": away.get("isWinner")},
                        {"name": home["team"]["name"], "homeAway": "home", "score": home.get("score"), "winner": home.get("isWinner")},
                    ],
                    "my_team": is_my_team(away["team"]["name"] + " " + home["team"]["name"], my_teams), "_start": start_dt,
                })
        status["sports"].append({"name": "MLB (official StatsAPI)", "ok": True, "events": len(games), "source": "statsapi.mlb.com", "key": "mlb"})
        return games
    except Exception as e:
        kind, code, detail, attempts = describe(e)
        status["sports"].append({"name": "MLB (official StatsAPI)", "ok": False, "error": f"{kind} {code or ''}: {detail}"[:200], "source": "statsapi.mlb.com", "key": "mlb"})
        log_failure("Sports data", "MLB (official StatsAPI)", url, kind, code, detail, attempts, "no MLB scores or schedule in the digest")
        return []


def collect_nhl(cfg, status):
    my_teams = cfg.get("my_teams", [])
    games, seen = [], set()
    url = ""
    try:
        for d in ((NOW - timedelta(days=2)).date(), NOW.date(), (NOW + timedelta(days=6)).date()):
            url = f"https://api-web.nhle.com/v1/schedule/{d.isoformat()}"
            data = json.loads(fetch(url))
            for day in data.get("gameWeek", []):
                for g in day.get("games", []):
                    if g.get("id") in seen:
                        continue
                    seen.add(g.get("id"))
                    a, h = g.get("awayTeam", {}), g.get("homeTeam", {})
                    an = (a.get("placeName", {}).get("default", "") + " " + a.get("commonName", {}).get("default", "")).strip() or a.get("abbrev", "")
                    hn = (h.get("placeName", {}).get("default", "") + " " + h.get("commonName", {}).get("default", "")).strip() or h.get("abbrev", "")
                    start_dt = parse_iso(g.get("startTimeUTC"))
                    gs = g.get("gameState", "")
                    state = {"FUT": "pre", "PRE": "pre", "LIVE": "in", "CRIT": "in", "OFF": "post", "FINAL": "post"}.get(gs, gs)
                    outcome = (g.get("gameOutcome") or {}).get("lastPeriodType")
                    games.append({
                        "id": g.get("id"), "start_utc": start_dt.isoformat() if start_dt else None, "start_et": et_str(start_dt),
                        "state": state, "status": gs + (f" ({outcome})" if outcome and outcome != "REG" and state == "post" else ""),
                        "venue": (g.get("venue") or {}).get("default"),
                        "broadcast": ", ".join(b.get("network", "") for b in g.get("tvBroadcasts", []) if b.get("network")) or None,
                        "competitors": [
                            {"name": an, "homeAway": "away", "score": a.get("score")},
                            {"name": hn, "homeAway": "home", "score": h.get("score")},
                        ],
                        "my_team": is_my_team(an + " " + hn, my_teams), "_start": start_dt,
                    })
        status["sports"].append({"name": "NHL (official schedule API)", "ok": True, "events": len(games), "source": "api-web.nhle.com", "key": "nhl"})
        return games
    except Exception as e:
        kind, code, detail, attempts = describe(e)
        status["sports"].append({"name": "NHL (official schedule API)", "ok": False, "error": f"{kind} {code or ''}: {detail}"[:200], "source": "api-web.nhle.com", "key": "nhl"})
        log_failure("Sports data", "NHL (official schedule API)", url, kind, code, detail, attempts, "NHL scores or schedule may be missing or partial in the digest")
        return games


# ---------- my teams: ESPN news and injuries ----------
def espn_base(sport, league):
    return f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}"


def find_espn_team(sport, league, name, groups=None):
    """Look up an ESPN team id by name, so no id is hard-coded. Returns (id, displayName)."""
    url = espn_base(sport, league) + "/teams?limit=1000" + (f"&groups={groups}" if groups else "")
    data = json.loads(fetch(url))
    teams = []
    for sp in data.get("sports", []):
        for lg in sp.get("leagues", []):
            for t in lg.get("teams", []):
                teams.append(t.get("team", t))
    want = name.lower()
    exact = [t for t in teams if (t.get("displayName") or "").lower() == want]
    part = [t for t in teams if want in (t.get("displayName") or "").lower()]
    pick = (exact or part or [None])[0]
    if not pick or not pick.get("id"):
        raise FetchError("NO_TEAM_MATCH", f"no team named '{name}' among {len(teams)} teams returned for {sport}/{league}")
    return str(pick["id"]), pick.get("displayName") or name


def shape_injury(inj):
    ath = inj.get("athlete") or {}
    pos = (ath.get("position") or {}).get("abbreviation") if isinstance(ath.get("position"), dict) else ath.get("position")
    d = parse_iso(inj.get("date"))
    return {"player": ath.get("displayName") or inj.get("name") or "unknown", "position": pos or "", "status": inj.get("status") or "",
            "date": et_str(d) if d else "", "comment": clean(inj.get("shortComment") or inj.get("longComment") or (inj.get("details") or {}).get("detail") or "", 200)}


def collect_teams(cfg, status):
    out, inj_cache = [], {}
    days = cfg.get("limits", {}).get("team_news_days", 7)
    cutoff = NOW - timedelta(days=days)
    for t in cfg.get("espn_teams", []):
        label = t.get("label") or t.get("name", "team")
        rec = {"label": label, "team": None, "team_id": None, "news": [], "injuries": [], "injuries_checked": False}
        st = {"name": label, "ok": False, "news_items": 0, "injuries": 0}
        try:
            tid, tname = find_espn_team(t["sport"], t["league"], t["name"], t.get("groups"))
            rec["team"], rec["team_id"] = tname, tid
            base = espn_base(t["sport"], t["league"])
        except Exception as e:
            kind, code, detail, attempts = describe(e)
            st["error"] = f"{kind} {code or ''}: {detail}"[:200]
            status["teams"].append(st)
            log_failure("Team data", label, "teams lookup", kind, code, detail, attempts, f"no team news or injuries for {label} in the digest")
            out.append(rec)
            continue
        news_url = f"{base}/news?team={tid}&limit=20"
        try:
            data = json.loads(fetch(news_url))
            for a in data.get("articles", []):
                pub = parse_iso(a.get("published") or a.get("lastModified"))
                if not pub or pub < cutoff:
                    continue
                link = ((a.get("links") or {}).get("web") or {}).get("href") or ""
                rec["news"].append({"title": clean(a.get("headline") or "", 300), "published": pub, "link": link,
                                    "snippet": clean(a.get("description") or "", 220)})
            rec["news"].sort(key=lambda x: x["published"], reverse=True)
            st["news_items"] = len(rec["news"])
            st["ok"] = True
        except Exception as e:
            kind, code, detail, attempts = describe(e)
            st["error"] = f"news: {kind} {code or ''}: {detail}"[:200]
            log_failure("Team data", label, news_url, kind, code, detail, attempts, f"no recent news for {label} in the digest")
        if t.get("injuries"):
            inj_url = f"{base}/injuries"
            try:
                if inj_url not in inj_cache:
                    inj_cache[inj_url] = json.loads(fetch(inj_url)).get("injuries", [])
                for block in inj_cache[inj_url]:
                    if str(block.get("id")) == tid or (block.get("displayName") or "").lower() == (tname or "").lower():
                        rec["injuries"] = [shape_injury(i) for i in block.get("injuries", [])]
                        break
                rec["injuries_checked"] = True
                st["injuries"] = len(rec["injuries"])
                st["ok"] = True
            except Exception as e:
                kind, code, detail, attempts = describe(e)
                st["error"] = (st.get("error", "") + f" injuries: {kind} {code or ''}: {detail}")[:200]
                log_failure("Team data", label, inj_url, kind, code, detail, attempts, f"no injury list for {label} in the digest")
        status["teams"].append(st)
        out.append(rec)
    return out


def render_teams(teams):
    lines = ["# Team digest (generated %s)" % et_str(NOW), "",
             "News headlines and injury lists for my teams from ESPN's unofficial endpoints. The injury list can lag the team's or league's "
             "official report, so confirm status with a second source before stating it. An empty list can mean 'no injuries listed' or "
             "'the endpoint returned nothing'; check the 'injuries checked' note.", ""]
    for r in teams:
        lines.append(f"## {r['label']}" + (f" (ESPN team id {r['team_id']})" if r.get("team_id") else " (team not found)"))
        if r["news"]:
            lines.append("News (last days, newest first):")
            for n in r["news"][:12]:
                snip = f" | {n['snippet']}" if n["snippet"] else ""
                lines.append(f"- {n['title']} | {et_str(n['published'])} | {n['link']}{snip}")
        else:
            lines.append("News: none returned")
        if r["injuries_checked"]:
            if r["injuries"]:
                lines.append("Injuries (ESPN list):")
                for i in r["injuries"]:
                    lines.append(f"- {i['player']} ({i['position']}) | {i['status']} | {i['date']} | {i['comment']}")
            else:
                lines.append("Injuries (ESPN list): none listed")
        lines.append("")
    return "\n".join(lines)



# ---------- health ----------
def compute_health(cfg, status):
    h = cfg.get("health", {})
    min_feeds = h.get("min_feeds_ok", 6)
    min_sports = h.get("min_sports_ok", 3)
    min_items = h.get("min_news_items", 30)
    feeds_ok = sum(1 for f in status["feeds"] if f["ok"])
    sports_ok = sum(1 for s in status["sports"] if s["ok"])
    items = sum(f.get("items_in_window", 0) for f in status["feeds"] if f["ok"])
    failed, degraded = [], []
    if feeds_ok == 0:
        failed.append("no news feed worked")
    elif items == 0:
        failed.append("feeds worked but returned no items in the window")
    if sports_ok == 0:
        failed.append("no sports source worked")
    if feeds_ok and feeds_ok < min_feeds:
        degraded.append(f"only {feeds_ok} news feeds worked (minimum {min_feeds})")
    if items and items < min_items:
        degraded.append(f"only {items} news items in the window (minimum {min_items})")
    if sports_ok and sports_ok < min_sports:
        degraded.append(f"only {sports_ok} sports sources worked (minimum {min_sports})")
    level = "FAILED" if failed else ("DEGRADED" if degraded else "OK")
    return {"level": level, "reasons": failed + degraded, "feeds_ok": feeds_ok, "sports_ok": sports_ok, "news_items": items}


# ---------- rendering ----------
def game_line(g):
    parts = []
    comps = g.get("competitors") or []
    two_team = len(comps) == 2 and all(c.get("homeAway") for c in comps)
    if two_team and g.get("state") in ("post", "in"):
        a, h = (comps[0], comps[1]) if comps[0].get("homeAway") == "away" else (comps[1], comps[0])
        parts.append(f"{a['name']} {a.get('score')} at {h['name']} {h.get('score')}")
    elif two_team:
        a, h = (comps[0], comps[1]) if comps[0].get("homeAway") == "away" else (comps[1], comps[0])
        parts.append(f"{a['name']} at {h['name']}")
    else:
        parts.append(g.get("name") or "event")
        shown = [f"{c['name']} ({c.get('score')})" if c.get("score") is not None else c["name"] for c in comps[:10]]
        if shown:
            parts.append("[" + "; ".join(shown) + "]")
    if two_team and any(c.get("rank") for c in comps):
        parts.append("ranks: " + ", ".join(f"{c['name']} #{c['rank']}" for c in comps if c.get("rank")))
    parts.append(f"{g.get('status') or ''} | {g.get('start_et')}")
    if g.get("broadcast"):
        parts.append("TV: " + g["broadcast"])
    if g.get("venue"):
        parts.append(g["venue"])
    for k in ("description", "series_status"):
        if g.get(k):
            parts.append(str(g[k]))
    flag = "[MY TEAM] " if g.get("my_team") else ""
    return "- " + flag + " | ".join(p for p in parts if p)


def render_sports(leagues):
    lines = ["# Sports digest (generated %s)" % et_str(NOW), "",
             "Source notes: ESPN scoreboard data (unofficial endpoint), official MLB StatsAPI and NHL schedule API. "
             "All times are Eastern. [MY TEAM] marks the Dolphins, Panthers, Heat, Gators, Chelsea or Marlins.", ""]
    finals_cut = NOW - timedelta(hours=FINAL_WINDOW_HOURS)
    horizon = NOW + timedelta(days=LOOKAHEAD_DAYS)
    for label, games in leagues.items():
        finals = sorted([g for g in games if g["state"] == "post" and g["_start"] and g["_start"] >= finals_cut], key=lambda g: g["_start"], reverse=True)
        live = [g for g in games if g["state"] == "in"]
        ahead = sorted([g for g in games if g["state"] == "pre" and g["_start"] and NOW - timedelta(hours=6) <= g["_start"] <= horizon], key=lambda g: g["_start"])
        if not (finals or live or ahead):
            continue
        lines.append(f"## {label}")
        if finals:
            lines.append("Finals (last ~40h):")
            lines += [game_line(g) for g in finals]
        if live:
            lines.append("Live now:")
            lines += [game_line(g) for g in live]
        if ahead:
            mine = [g for g in ahead if g["my_team"]]
            others = [g for g in ahead if not g["my_team"]]
            lines.append("Next 7 days, my teams:" if mine else "Next 7 days: no game for my teams in this league's data")
            lines += [game_line(g) for g in mine]
            if others:
                lines.append(f"Next 7 days, others (first {OTHERS_PER_LEAGUE}):")
                lines += [game_line(g) for g in others[:OTHERS_PER_LEAGUE]]
        lines.append("")
    return "\n".join(lines)


def render_news(news):
    lines = ["# News headlines digest (generated %s)" % et_str(NOW), "",
             "Headlines and short publisher snippets from public RSS/Atom feeds, last %d hours, newest first. "
             "Each line: title | published (ET) | link | snippet." % NEWS_WINDOW_HOURS, ""]
    for feed in news:
        if not feed["items"]:
            continue
        lines.append(f"## {feed['source']}")
        for i in feed["items"][:MD_ITEMS_PER_FEED]:
            snippet = f" | {i['snippet']}" if i["snippet"] else ""
            lines.append(f"- {i['title']} | {et_str(i['published'])} | {i['link']}{snippet}")
        lines.append("")
    return "\n".join(lines)


STOP = set("the a an and or of to in on for at by with from as is are was were be been it its this that these those after over into about new says say said will would could has have had not but more than amid out up down his her their our your you who what when how why".split())


def title_tokens(t):
    return {w for w in re.findall(r"[a-z0-9']+", t.lower()) if len(w) > 2 and w not in STOP}


ORG_SUFFIX = re.compile(r"\s*(\([^)]*\)|world|politics|business|technology|markets)\s*$", re.I)


def org_name(src):
    prev = None
    while prev != src:
        prev, src = src, ORG_SUFFIX.sub("", src).strip()
    return src or prev


BROADCAST = re.compile(r"^\s*\d{1,2}/\d{1,2}\s*:")


def category_label(raw):
    c = re.sub(r"\s*\(.*?\)", "", raw or "").strip()
    return "General" if c.lower() == "core" or not c else c


def render_brief(news, max_per_cat=30, max_single=10, single_by_cat=None):
    single_by_cat = single_by_cat or {"Sports": 30}
    """Condensed view: items clustered by headline-word overlap; clusters carried by more outlets rank first.
    'Outlets' counts feeds, NOT independent sources: outlets often repeat the same wire story."""
    items = []
    for feed in news:
        for i in feed["items"]:
            if BROADCAST.match(i["title"]):
                continue  # TV/podcast episode listings such as "10/2: CBS Evening News"
            items.append({"src": feed["source"], "org": org_name(feed["source"]), "cat": category_label(feed.get("category", "")), **i})
    items.sort(key=lambda i: i["published"], reverse=True)
    clusters = []
    for it in items:
        toks = title_tokens(it["title"])
        best = None
        for c in clusters:
            inter = len(toks & c["toks"])
            if toks and inter >= 2 and inter / min(len(toks), len(c["seed"])) >= 0.5:
                best = c
                break
        if best is None:
            clusters.append({"toks": set(toks), "seed": set(toks), "cat": it["cat"], "items": [it]})
        else:
            best["items"].append(it)
    by_cat = {}
    for c in clusters:
        c["outlets"] = sorted({x["org"] for x in c["items"]})
        cats = [x["cat"] for x in c["items"]]
        c["cat"] = max(set(cats), key=lambda k: (cats.count(k), k == "General"))
        by_cat.setdefault(c["cat"], []).append(c)
    lines = ["# Condensed news digest (generated %s)" % et_str(NOW), "",
             "Headlines grouped by similar wording. 'Outlets' = number of distinct news organizations carrying the story (CBS News and CBS News World count once); this is NOT a count of independent "
             "sources (outlets often repeat the same wire copy). Matching is by headline words, so a few related stories may be split or merged. "
             "Window: last %d hours. Full list: digest_news.md." % NEWS_WINDOW_HOURS, ""]
    for cat in sorted(by_cat):
        cl = by_cat[cat]
        multi = sorted([c for c in cl if len(c["outlets"]) >= 2], key=lambda c: (-len(c["outlets"]), -max(x["published"] for x in c["items"]).timestamp()))
        single = [c for c in cl if len(c["outlets"]) < 2]
        lines.append(f"## {cat}")
        for c in multi[:max_per_cat]:
            lead = c["items"][0]
            snip = f" | {lead['snippet']}" if lead.get("snippet") else ""
            lines.append(f"- [{len(c['outlets'])} outlets: {', '.join(c['outlets'][:6])}] {lead['title']} | {et_str(lead['published'])} | {lead['link']}{snip}")
        for c in single[:single_by_cat.get(cat, max_single)]:
            lead = c["items"][0]
            lines.append(f"- [1 outlet: {lead['org']}] {lead['title']} | {et_str(lead['published'])} | {lead['link']}")
        lines.append("")
    return "\n".join(lines)


def render_status(status, health):
    ok_f = [f for f in status["feeds"] if f["ok"]]
    ok_s = [s for s in status["sports"] if s["ok"]]
    lines = ["# Collector status", "",
             f"HEALTH: {health['level']}",
             "HEALTH_REASONS: " + ("; ".join(health["reasons"]) if health["reasons"] else "none"),
             f"GENERATED_UTC: {NOW.strftime('%Y-%m-%dT%H:%M:%SZ')}", f"GENERATED_ET: {et_str(NOW)}",
             f"COLLECTOR_VERSION: {COLLECTOR_VERSION}", "",
             f"News feeds OK: {health['feeds_ok']} of {len(status['feeds'])} ({health['news_items']} items in window). "
             f"Sports sources OK: {health['sports_ok']} of {len(status['sports'])}.", ""]
    if ok_f:
        lines.append("Feeds that worked (items in last %dh; newest item):" % NEWS_WINDOW_HOURS)
        lines += [f"- {f['name']}: {f['items_in_window']} items; newest {f['newest']}" for f in ok_f]
        lines.append("")
    if ok_s:
        lines.append("Sports sources that worked:")
        lines += [f"- {s['name']}: {s['events']} events" for s in ok_s]
        lines.append("")
    if status.get("teams"):
        lines.append("Team data (ESPN news and injuries):")
        for t in status["teams"]:
            lines.append(f"- {t['name']}: " + (f"{t.get('news_items', 0)} news items, {t.get('injuries', 0)} injury entries" if t["ok"] else "FAILED") + (f" | {t['error']}" if t.get("error") else ""))
        lines.append("")
    lines.append("## Failures and warnings this run (full detail; the same lines go to failure_log.md)")
    lines += [format_record(r) for r in FAILURES] if FAILURES else ["- none"]
    lines.append("")
    if status.get("probe"):
        lines += ["## Article access probe (one article per outlet; page text is measured and discarded)",
                  "Shows whether the COLLECTOR can read an article page from this runner. It does not show what the briefing task's own tools can read.", "",
                  "| Outlet | Result | HTTP | Visible text chars | Seconds | Note |", "|---|---|---|---|---|---|"]
        for p in status["probe"]:
            note = p.get("note") or p.get("error") or ""
            lines.append(f"| {p['source']} | {p['result']} | {p.get('http_code') or ''} | {p.get('text_chars', '')} | {p.get('seconds', '')} | {note} |")
        lines.append("")
    return "\n".join(lines)


def update_rolling_log(status, health, keep=LOG_KEEP_RUNS):
    path = "failure_log.md"
    header = ("# Rolling failure log\n\nOne block per run, oldest first (last %d runs kept). Each line: time ET | severity | step | source | "
              "URL | error | attempts | impact | likely cause. Error text is exact; \"likely cause\" is INFERRED unless it says VERIFIED.\n" % keep)
    blocks = []
    try:
        with open(path, encoding="utf-8") as f:
            old = f.read()
        blocks = ["## Run " + p.strip() for p in old.split("\n## Run ")[1:]]
    except FileNotFoundError:
        pass
    block = [f"## Run {et_str(NOW)} | HEALTH {health['level']} | feeds {health['feeds_ok']}/{len(status['feeds'])} | sports {health['sports_ok']}/{len(status['sports'])}"]
    block += [format_record(r) for r in FAILURES] if FAILURES else ["- no failures or warnings"]
    blocks.append("\n".join(block))
    blocks = blocks[-keep:]
    with open(path, "w", encoding="utf-8") as f:
        f.write(header + "\n" + "\n\n".join(blocks) + "\n")


def strip_private(obj):
    if isinstance(obj, dict):
        return {k: strip_private(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [strip_private(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


def main():
    global NEWS_WINDOW_HOURS, MD_ITEMS_PER_FEED, JSON_ITEMS_PER_FEED, OTHERS_PER_LEAGUE, NEWS_BUDGET_SECONDS, PROBE_BUDGET_SECONDS
    status = {"feeds": [], "sports": [], "teams": [], "probe": []}
    try:
        with open("config.json", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as e:
        log_failure("Config", "config.json", "config.json", "CONFIG_ERROR", None, f"{type(e).__name__}: {e}", 1,
                    "no collection happened this run; digest_news/digest_sports/digest.json were NOT refreshed")
        health = {"level": "FAILED", "reasons": ["config.json could not be read"], "feeds_ok": 0, "sports_ok": 0, "news_items": 0}
        with open("digest_status.md", "w", encoding="utf-8") as f:
            f.write(render_status(status, health))
        update_rolling_log(status, health)
        print("FAILED: config.json could not be read")
        sys.exit(1)

    lim = cfg.get("limits", {})
    NEWS_WINDOW_HOURS = lim.get("news_window_hours", NEWS_WINDOW_HOURS)
    MD_ITEMS_PER_FEED = lim.get("md_items_per_feed", MD_ITEMS_PER_FEED)
    JSON_ITEMS_PER_FEED = lim.get("json_items_per_feed", JSON_ITEMS_PER_FEED)
    OTHERS_PER_LEAGUE = lim.get("others_per_league", OTHERS_PER_LEAGUE)
    NEWS_BUDGET_SECONDS = lim.get("news_budget_seconds", NEWS_BUDGET_SECONDS)
    PROBE_BUDGET_SECONDS = lim.get("probe_budget_seconds", PROBE_BUDGET_SECONDS)

    news = collect_news(cfg, status)
    leagues = collect_espn(cfg, status)
    leagues["MLB (official StatsAPI)"] = collect_mlb(cfg, status)
    leagues["NHL (official schedule API)"] = collect_nhl(cfg, status)
    teams = collect_teams(cfg, status)
    health = compute_health(cfg, status)

    # Write the news and sports digests BEFORE the slower article probe, so a probe problem can never cost the digest.
    with open("digest_news.md", "w", encoding="utf-8") as f:
        f.write(render_news(news))
    with open("digest_sports.md", "w", encoding="utf-8") as f:
        f.write(render_sports(leagues))
    with open("digest_brief.md", "w", encoding="utf-8") as f:
        f.write(render_brief(news))
    with open("digest_teams.md", "w", encoding="utf-8") as f:
        f.write(render_teams(teams))

    collect_probe(cfg, status, news)

    with open("digest_status.md", "w", encoding="utf-8") as f:
        f.write(render_status(status, health))
    with open("digest.json", "w", encoding="utf-8") as f:
        json.dump(strip_private({"generated_utc": NOW, "health": health, "status": status, "failures": FAILURES, "news": news, "sports": leagues, "teams": teams}),
                  f, ensure_ascii=False, indent=1)
    update_rolling_log(status, health)

    print(f"done: HEALTH {health['level']} | feeds {health['feeds_ok']}/{len(status['feeds'])} | sports {health['sports_ok']}/{len(status['sports'])} | {len(FAILURES)} failure/warning lines")
    if health["level"] == "FAILED":
        sys.exit(1)  # make the GitHub run show as failed


if __name__ == "__main__":
    main()
