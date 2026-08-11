#!/usr/bin/env python3
"""
stock_alert_bot.py  (Finnhub edition)
-------------------------------------
Real-time stock-alert bot that posts to a Telegram channel.

Alert types:
  1. Trading HALTS / resumes  -> Nasdaq Trader Trade-Halt RSS (free, official)
  2. PRICE MOVERS / spikes     -> Finnhub /quote per watchlist symbol (free tier)
  3. BREAKING NEWS            -> Finnhub /company-news per watchlist symbol (free tier)

Why Finnhub: the free tier (60 calls/min, no card) covers real-time quotes and
company news, which is enough to run a watchlist scanner at no cost. NOTE: the
free tier is licensed for personal / non-commercial use — upgrade to a paid
Finnhub plan before charging members.

True volume-surge detection needs candle data (paid) or the Finnhub trades
WebSocket (free, 50 symbols). This build approximates "spikes" via large % moves;
see the check_movers() note for where to add the WebSocket later.

--------------------------------------------------------------------------
QUICK START
--------------------------------------------------------------------------
  pip install requests feedparser
  export TELEGRAM_BOT_TOKEN="123456:ABC..."     # from @BotFather
  export TELEGRAM_CHAT_ID="-1001234567890"       # your channel id
  export FINNHUB_API_KEY="your_finnhub_key"      # finnhub.io (free)
  python stock_alert_bot.py                        # live
  python stock_alert_bot.py --test                 # dry run: prints, never sends

On Railway these are set as service Variables (same names, no quotes).
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
def _env(*names):
    """Return the first non-empty env var among names, trimmed of stray quotes."""
    for n in names:
        v = os.getenv(n)
        if v:
            return v.strip().strip('"').strip("'")
    return ""


TELEGRAM_BOT_TOKEN = _env("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = _env("TELEGRAM_CHAT_ID")
FINNHUB_API_KEY    = _env("FINNHUB_API_KEY")

# Tickers to scan for price moves + news. Keep focused for the free 60/min limit.
WATCHLIST = ["AAPL", "TSLA", "NVDA", "AMD", "PLTR", "SOFI", "MARA", "RIOT"]

# Alert thresholds
MOVERS_MIN_CHANGE = 5.0    # alert when a watchlist symbol moves >= this % (abs)
MIN_PRICE         = 1.00   # ignore anything under this price

# Poll intervals (seconds). Halts are the most time-sensitive.
INTERVAL_HALTS  = 20
INTERVAL_MOVERS = 60
INTERVAL_NEWS   = 180

NASDAQ_HALT_RSS = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
FINNHUB_BASE    = "https://finnhub.io/api/v1"

DRY_RUN = "--test" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("alertbot")

# De-dup memory so the same event isn't posted twice in a session.
_seen = set()


# ----------------------------------------------------------------------
# TELEGRAM
# ----------------------------------------------------------------------
def send_telegram(text):
    """Send an HTML-formatted message to the configured Telegram channel."""
    if DRY_RUN:
        print("\n--- ALERT (dry run) ---\n" + text + "\n-----------------------")
        return
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured; skipping send.")
        return
    url = "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN + "/sendMessage"
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


def once(key):
    """Return True the first time a given event key is seen, False after."""
    if key in _seen:
        return False
    _seen.add(key)
    return True


# ----------------------------------------------------------------------
# FINNHUB HELPERS
# ----------------------------------------------------------------------
def finnhub_get(path, params=None):
    if not FINNHUB_API_KEY:
        log.warning("FINNHUB_API_KEY not set; skipping %s", path)
        return None
    params = dict(params or {})
    params["token"] = FINNHUB_API_KEY
    try:
        r = requests.get(FINNHUB_BASE + "/" + path, params=params, timeout=15)
        if r.status_code == 429:
            log.warning("Finnhub rate limit hit on %s; backing off", path)
            time.sleep(2)
            return None
        if r.status_code != 200:
            log.error("Finnhub %s -> %s: %s", path, r.status_code, r.text[:150])
            return None
        return r.json()
    except (requests.RequestException, ValueError) as e:
        log.error("Finnhub request failed (%s): %s", path, e)
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
    except Exception as e:  # noqa: BLE001
        log.error("Halt feed error: %s", e)
        return
    for entry in feed.entries:
        eid = entry.get("id") or entry.get("link") or entry.get("title", "")
        if not eid or not once("halt:" + eid):
            continue
        title = html.escape(entry.get("title", "Trading halt"))
        summary = html.escape(entry.get("summary", ""))[:300]
        send_telegram("\U0001F6A8 <b>TRADING HALT</b>\n" + title + "\n" + summary)


# ----------------------------------------------------------------------
# ALERT: PRICE MOVERS / SPIKES  (Finnhub /quote per watchlist symbol)
# ----------------------------------------------------------------------
def check_movers():
    """
    Free-tier scanner: pull a real-time quote for each watchlist symbol and
    alert when the day's percent change crosses the threshold.

    To upgrade to true VOLUME-SURGE detection later, subscribe to the Finnhub
    trades WebSocket (wss://ws.finnhub.io?token=KEY), accumulate per-symbol
    traded volume in a rolling window, and fire when it exceeds N x the average.
    That needs the `websocket-client` package and a background thread.
    """
    for sym in WATCHLIST:
        q = finnhub_get("quote", {"symbol": sym})
        if not isinstance(q, dict):
            continue
        try:
            price = float(q.get("c") or 0)      # current price
            chg = float(q.get("dp") or 0)       # percent change
        except (TypeError, ValueError):
            continue
        if price < MIN_PRICE:
            continue
        if abs(chg) >= MOVERS_MIN_CHANGE:
            day = time.strftime("%Y%m%d")
            if not once("mover:" + sym + ":" + day):
                continue
            arrow = "\U0001F4C8" if chg >= 0 else "\U0001F4C9"
            label = "TOP GAINER" if chg >= 0 else "TOP LOSER"
            send_telegram(
                "\U0001F525 <b>" + label + "</b> " + arrow + "\n"
                + "<b>" + html.escape(sym) + "</b>  $" + format(price, ",.2f")
                + "  (" + format(chg, "+.1f") + "%)"
            )
        # tiny pause to stay well under 60 calls/min across the watchlist
        time.sleep(0.3)


# ----------------------------------------------------------------------
# ALERT: BREAKING NEWS  (Finnhub /company-news per watchlist symbol)
# ----------------------------------------------------------------------
def check_news():
    today = time.strftime("%Y-%m-%d")
    for sym in WATCHLIST:
        items = finnhub_get("company-news", {"symbol": sym, "from": today, "to": today})
        if not isinstance(items, list):
            continue
        for n in items[:5]:
            url = n.get("url") or ""
            nid = str(n.get("id") or url)
            if not nid or not once("news:" + nid):
                continue
            headline = html.escape(str(n.get("headline", "")))
            source = html.escape(str(n.get("source", "")))
            send_telegram(
                "\U0001F4F0 <b>NEWS</b> " + html.escape(sym) + "\n"
                + headline + "\n<i>" + source + "</i>\n" + url
            )
        time.sleep(0.3)


# ----------------------------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------------------------
def main():
    log.info("Starting alert bot (dry_run=%s). Watchlist: %s", DRY_RUN, ", ".join(WATCHLIST))
    if DRY_RUN:
        for fn in (check_halts, check_movers, check_news):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                log.error("%s failed: %s", fn.__name__, e)
        log.info("Dry run complete.")
        return

    schedule = [
        (check_halts,  INTERVAL_HALTS),
        (check_movers, INTERVAL_MOVERS),
        (check_news,   INTERVAL_NEWS),
    ]
    next_run = {fn.__name__: 0.0 for fn, _ in schedule}

    while True:
        now = time.time()
        for fn, interval in schedule:
            if now >= next_run[fn.__name__]:
                try:
                    fn()
                except Exception as e:  # noqa: BLE001 - never let one feed kill the loop
                    log.error("%s failed: %s", fn.__name__, e)
                next_run[fn.__name__] = now + interval
        time.sleep(1)


if __name__ == "__main__":
    main()
