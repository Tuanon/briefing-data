# briefing-data

Data collector for a personal daily news briefing. A scheduled GitHub Actions job runs `collect.py`,
which reads public RSS/Atom feeds that publishers offer and public sports score endpoints, then commits:

- `digest_status.md`: HEALTH flag (OK / DEGRADED / FAILED), generation time, which sources worked, every failure in detail, and the article-access probe (read first)
- `digest_news.md`: recent headlines with short publisher snippets and links
- `digest_brief.md`: condensed headlines grouped by story, most-carried first
- `digest_sports.md`: finals, live games and the next 7 days by league (my teams flagged)
- `digest_teams.md`: per-team news headlines and injury lists for my teams (ESPN endpoints)
- `digest.json`: the same data, structured
- `failure_log.md`: rolling log of failures and warnings, one block per run (last 45 runs)

Personal use only. One run per day (plus one backup run). The optional article probe (off by default in config.json) fetches one article per outlet (respecting robots.txt),
measures it and discards it; no article text is stored.

## Add a feed
1. Open `config.json` and click the pencil icon.
2. Under `news_feeds`, add `{"name": "Outlet", "url": "https://..."},` (comma after every entry except the last).
3. Commit. Then Actions tab, collect-briefing-data, Run workflow, and check `digest_status.md`.

There is no cap on the number of feeds. The ESPN scoreboard endpoint is unofficial and may change without notice.
