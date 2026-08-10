#!/usr/bin/env python3
"""
stock_alert_bot.py
------------------
Starter real-time stock-alert bot that posts to a Telegram channel.

Covers four alert types out of the box:
  1. Trading HALTS / resumes   -> Nasdaq Trader Trade-Halt RSS (free, official)
  2. VOLUME SPIKES             -> Financial Modeling Prep (FMP) batch quotes vs avg volume
  3. TOP MOVERS               -> FMP biggest-gainers / biggest-losers
  4. BREAKING NEWS            -> FMP stock news for your watchlist

Design goals: dependency-light, single file, safe to run on a free host
(Render / Railway / Fly.io / any $5 VPS). Runs one loop; each feed has its
own interval so halts are checked far more often than news.

--------------------------------------------------------------------------
QUICK START
--------------------------------------------------------------------------
  pip install requests feedparser
  export TELEGRAM_BOT_TOKEN="123456:ABC..."      # from @BotFather
  export TELEGRAM_CHAT_ID="-1001234567890"        # your channel id
  export FMP_API_KEY="your_fmp_key"               # financialmodelingprep.com
  python stock_alert_bot.py                        # live
  python stock_alert_bot.py --test                 # dry run: prints, never sends

Tune the CONFIG block below (watchlist, thresholds, intervals).
This is a scaffold to build on, not investment advice. See the build plan.
--------------------------------------------------------------------------
"""

import os
import sys
import time
import html
import logging

import requests

try:
    import feedparser  # only needed for the halt RSS feed
except ImportError:
    feedparser = None

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
FMP_API_KEY        = os.getenv("FMP_API_KEY", "")

# Tickers to watch for volume spikes + news. Keep this focused for the
# free tiers; expand as you upgrade your data plan.
WATCHLIST = ["AAPL", "TSLA", "NVDA", "AMD", "PLTR", "SOFI", "MARA", "RIOT"]

# Alert thresholds
RVOL_THRESHOLD      = 3.0    # fire volume spike when today's volume >= 3x the 50-day avg
MIN_PRICE           = 1.00   # ignore sub-$1 unless you specifically want them
MIN_ABS_CHANGE_PCT  = 5.0    # only volume-spike alert if price also moved >= 5%
MOVERS_MIN_CHANGE   = 10.0   # only post top movers up/down at least this %

# Poll intervals (seconds). Halts are the most time-sensitive.
INTERVAL_HALTS   = 20
INTERVAL_VOLUME  = 60
INTERVAL_MOVERS  = 300
INTERVAL_NEWS    = 180

NASDAQ_HALT_RSS = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
FMP_BASE        = "https://financialmodelingprep.com/stable"
EDGAR_UA        = "YourCompany StockAlerts contact@yourdomain.com"  # required by SEC if you add EDGAR

DRY_RUN = "--test" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("alertbot")

# De-dup memory so the same event isn't posted twice in a session.
_seen = set()


# ----------------------------------------------------------------------
# TELEGRAM
# ----------------------------------------------------------------------
def send_telegram(text: str) -> None:
    """Send an HTML-formatted message to the configured Telegram channel."""
    if DRY_RUN:
        print("\n--- ALERT (dry run) ---\n" + text + "\n-----------------------")
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured; skipping send.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            log.error("Telegram error %s: %s", r.status_code, r.text[:200])
    except requests.RequestException as e:
        log.error("Telegram request failed: %s", e)


def once(key: str) -> bool:
    """Return True the first time a given event key is seen, False after."""
    if key in _seen:
        return False
    _seen.add(key)
    return True


# ----------------------------------------------------------------------
# FMP HELPERS
# ----------------------------------------------------------------------
def fmp_get(path: str, params: dict | None = None):
    if not FMP_API_KEY:
        log.warning("FMP_API_KEY not set; skipping %s", path)
        return None
    params = dict(params or {})
    params["apikey"] = FMP_API_KEY
    try:
        r = requests.get(f"{FMP_BASE}/{path}", params=params, timeout=15)
        if r.status_code != 200:
            log.error("FMP %s -> %s: %s", path, r.status_code, r.text[:150])
            return None
        return r.json()
    except (requests.RequestException, ValueError) as e:
        log.error("FMP request failed (%s): %s", path, e)
        return None


# ----------------------------------------------------------------------
# ALERT: TRADING HALTS  (Nasdaq Trader RSS, free)
# ----------------------------------------------------------------------
def check_halts():
    if feedparser is None:
        log.warning("feedparser not installed; skipping halts. pip install feedparser")
        return
    try:
        feed = feedparser.parse(NASDAQ_HALT_RSS)
    except Exception as e:  # noqa: BLE001 - feedparser can raise various things
        log.error("Halt feed error: %s", e)
        return
    for entry in feed.entries:
        eid = entry.get("id") or entry.get("link") or entry.get("title", "")
        if not eid or not once("halt:" + eid):
            continue
        title = html.escape(entry.get("title", "Trading halt"))
        summary = html.escape(entry.get("summary", ""))[:300]
        send_telegram(f"🚨 <b>TRADING HALT</b>\n{title}\n{summary}")


# ----------------------------------------------------------------------
# ALERT: VOLUME SPIKES  (FMP batch quotes)
# ----------------------------------------------------------------------
def check_volume():
    if not WATCHLIST:
        return
    data = fmp_get("batch-quote", {"symbols": ",".join(WATCHLIST)})
    if not isinstance(data, list):
        return
    for q in data:
        try:
            sym = q["symbol"]
            price = float(q.get("price") or 0)
            vol = float(q.get("volume") or 0)
            avg = float(q.get("avgVolume") or 0)
            chg = float(q.get("changePercentage") or q.get("changesPercentage") or 0)
        except (KeyError, TypeError, ValueError):
            continue
        if price < MIN_PRICE or avg <= 0:
            continue
        rvol = vol / avg
        if rvol >= RVOL_THRESHOLD and abs(chg) >= MIN_ABS_CHANGE_PCT:
            # Only alert once per day per symbol (bucket key by date).
            day = time.strftime("%Y%m%d")
            if not once(f"vol:{sym}:{day}"):
                continue
            arrow = "📈" if chg >= 0 else "📉"
            send_telegram(
                f"🔥 <b>VOLUME SPIKE</b> {arrow}\n"
                f"<b>{html.escape(sym)}</b>  ${price:,.2f}  ({chg:+.1f}%)\n"
                f"Volume {vol:,.0f} = <b>{rvol:.1f}x</b> avg"
            )


# ----------------------------------------------------------------------
# ALERT: TOP MOVERS  (FMP gainers / losers)
# ----------------------------------------------------------------------
def check_movers():
    for path, label, emoji in (
        ("biggest-gainers", "TOP GAINER", "📈"),
        ("biggest-losers", "TOP LOSER", "📉"),
    ):
        data = fmp_get(path)
        if not isinstance(data, list):
            continue
        for m in data[:15]:
            try:
                sym = m["symbol"]
                price = float(m.get("price") or 0)
                chg = float(m.get("changesPercentage") or m.get("changePercentage") or 0)
            except (KeyError, TypeError, ValueError):
                continue
            if price < MIN_PRICE or abs(chg) < MOVERS_MIN_CHANGE:
                continue
            day = time.strftime("%Y%m%d")
            if not once(f"mover:{sym}:{day}"):
                continue
            send_telegram(
                f"{emoji} <b>{label}</b>\n"
                f"<b>{html.escape(sym)}</b>  ${price:,.2f}  ({chg:+.1f}%)"
            )


# ----------------------------------------------------------------------
# ALERT: BREAKING NEWS  (FMP stock news for watchlist)
# ----------------------------------------------------------------------
def check_news():
    if not WATCHLIST:
        return
    data = fmp_get("news/stock", {"symbols": ",".join(WATCHLIST), "limit": 20})
    if not isinstance(data, list):
        return
    for n in data:
        url = n.get("url") or ""
        if not url or not once("news:" + url):
            continue
        sym = html.escape(str(n.get("symbol", "")))
        title = html.escape(str(n.get("title", "")))
        site = html.escape(str(n.get("site", n.get("publisher", ""))))
        send_telegram(f"📰 <b>NEWS</b> {sym}\n{title}\n<i>{site}</i>\n{url}")


# ----------------------------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------------------------
def main():
    log.info("Starting alert bot (dry_run=%s). Watchlist: %s", DRY_RUN, ", ".join(WATCHLIST))
    if DRY_RUN:
        # One pass of each check so you can see output immediately.
        for fn in (check_halts, check_volume, check_movers, check_news):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                log.error("%s failed: %s", fn.__name__, e)
        log.info("Dry run complete.")
        return

    schedule = [
        (check_halts,  INTERVAL_HALTS,  0.0),
        (check_volume, INTERVAL_VOLUME, 0.0),
        (check_movers, INTERVAL_MOVERS, 0.0),
        (check_news,   INTERVAL_NEWS,   0.0),
    ]
    next_run = {fn.__name__: 0.0 for fn, _, _ in schedule}

    while True:
        now = time.time()
        for fn, interval, _ in schedule:
            if now >= next_run[fn.__name__]:
                try:
                    fn()
                except Exception as e:  # noqa: BLE001 - never let one feed kill the loop
                    log.error("%s failed: %s", fn.__name__, e)
                next_run[fn.__name__] = now + interval
        time.sleep(1)


if __name__ == "__main__":
    main()
