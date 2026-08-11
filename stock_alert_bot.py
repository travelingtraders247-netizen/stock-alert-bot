#!/usr/bin/env python3
"""
stock_alert_bot.py  (Finnhub edition + real-time volume surge)
--------------------------------------------------------------
Real-time stock-alert bot that posts to a Telegram channel.

Alert types:
  1. Trading HALTS / resumes  -> Nasdaq Trader Trade-Halt RSS (free, official)
  2. VOLUME SURGE             -> Finnhub trades WebSocket (free, up to 50 symbols)
  3. PRICE MOVERS / spikes     -> Finnhub /quote per watchlist symbol (free tier)
  4. BREAKING NEWS            -> Finnhub /company-news per watchlist symbol (free tier)

Everything here runs on Finnhub's FREE tier (60 REST calls/min + a trades
WebSocket for up to 50 symbols) plus the free Nasdaq halt feed. NOTE: the free
tier is licensed for personal / non-commercial use -- upgrade to a paid Finnhub
plan before charging members.

--------------------------------------------------------------------------
QUICK START
--------------------------------------------------------------------------
  pip install requests feedparser websocket-client
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
import json
import time
import html
import logging
import threading
from collections import defaultdict, deque

import requests

try:
    import feedparser  # halt RSS feed
except ImportError:
    feedparser = None

try:
    import websocket  # from the `websocket-client` package (volume surge)
except ImportError:
    websocket = None


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

# Tickers to scan. Finnhub's free WebSocket allows up to 50 symbols.
WATCHLIST = ["AAPL", "TSLA", "NVDA", "AMD", "PLTR", "SOFI", "MARA", "RIOT"]

# Price-mover thresholds (REST /quote)
MOVERS_MIN_CHANGE = 5.0    # alert when a symbol moves >= this % (abs)
MIN_PRICE         = 1.00   # ignore anything under this price

# Volume-surge thresholds (WebSocket trades)
VOL_BUCKET_SEC = 60        # size of each volume bucket (seconds)
VOL_HISTORY    = 20        # how many past buckets form the baseline average
VOL_MIN_SAMPLES = 5        # need this many baseline buckets before alerting
VOL_SURGE_MULT = 3.0       # fire when a bucket's volume >= this x the baseline avg
VOL_MIN_SHARES = 5000      # ignore tiny buckets to avoid noise on thin names

# Poll intervals (seconds). Halts are the most time-sensitive.
INTERVAL_HALTS   = 20
INTERVAL_MOVERS  = 60
INTERVAL_NEWS    = 180
INTERVAL_VOLROLL = VOL_BUCKET_SEC

NASDAQ_HALT_RSS = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
FINNHUB_BASE    = "https://finnhub.io/api/v1"
FINNHUB_WS      = "wss://ws.finnhub.io?token="

DRY_RUN = "--test" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("alertbot")

# De-dup memory so the same event isn't posted twice in a session.
_seen = set()

# Global send pacing so a startup backlog never trips Telegram's flood limit.
_send_lock = threading.Lock()
_last_send_ts = [0.0]
MIN_SEND_GAP = 1.2      # minimum seconds between two Telegram messages
_silent = False         # when True, sends are suppressed (used to prime de-dup)

# Volume-surge shared state (written by the WS thread, read by the rollup).
_vol_lock = threading.Lock()
_vol_current = defaultdict(float)                       # symbol -> volume this bucket
_vol_history = defaultdict(lambda: deque(maxlen=VOL_HISTORY))  # symbol -> past buckets


# ----------------------------------------------------------------------
# TELEGRAM
# ----------------------------------------------------------------------
def send_telegram(text):
    """Send an HTML message to Telegram, paced so we never trip the flood limit.

    All sends are serialized through _send_lock and spaced at least
    MIN_SEND_GAP apart. A 429 (Too Many Requests) is honored by sleeping the
    server-provided retry_after and retrying, so alerts queue instead of drop.
    """
    if _silent:
        return
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
    with _send_lock:
        gap = time.time() - _last_send_ts[0]
        if gap < MIN_SEND_GAP:
            time.sleep(MIN_SEND_GAP - gap)
        for _attempt in range(4):
            try:
                r = requests.post(url, json=payload, timeout=15)
            except requests.RequestException as e:
                log.error("Telegram request failed: %s", e)
                break
            if r.status_code == 429:
                retry = 5
                try:
                    retry = int(r.json()["parameters"]["retry_after"])
                except (ValueError, KeyError, TypeError):
                    pass
                log.warning("Telegram 429; sleeping %ss then retrying", retry)
                time.sleep(retry + 1)
                continue
            if r.status_code != 200:
                log.error("Telegram error %s: %s", r.status_code, r.text[:200])
            break
        _last_send_ts[0] = time.time()


def once(key):
    """Return True the first time a given event key is seen, False after."""
    if key in _seen:
        return False
    _seen.add(key)
    return True


# ----------------------------------------------------------------------
# FINNHUB REST HELPER
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
        time.sleep(0.3)  # stay under 60 calls/min across the watchlist


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
# ALERT: VOLUME SURGE  (Finnhub trades WebSocket, real-time)
# ----------------------------------------------------------------------
def _ws_on_message(ws, message):
    """Accumulate traded volume per symbol from streaming trade ticks."""
    try:
        data = json.loads(message)
    except (ValueError, TypeError):
        return
    if data.get("type") != "trade":
        return
    with _vol_lock:
        for t in data.get("data", []):
            sym = t.get("s")
            vol = t.get("v") or 0
            if sym:
                _vol_current[sym] += float(vol)


def _ws_on_open(ws):
    for sym in WATCHLIST:
        try:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
        except Exception as e:  # noqa: BLE001
            log.error("WS subscribe failed for %s: %s", sym, e)
    log.info("Volume WebSocket connected; subscribed to %d symbols", len(WATCHLIST))


def _ws_on_error(ws, err):
    log.warning("Volume WebSocket error: %s", err)


def _ws_run():
    """Background thread: keep a Finnhub trades WebSocket alive with reconnect."""
    if websocket is None:
        log.warning("websocket-client not installed; volume surge disabled.")
        return
    if not FINNHUB_API_KEY:
        log.warning("FINNHUB_API_KEY not set; volume surge disabled.")
        return
    url = FINNHUB_WS + FINNHUB_API_KEY
    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=_ws_on_open,
                on_message=_ws_on_message,
                on_error=_ws_on_error,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:  # noqa: BLE001
            log.error("Volume WebSocket crashed: %s", e)
        time.sleep(5)  # reconnect backoff


def check_volume_surge():
    """
    Called once per bucket. Compare the just-finished bucket's volume to the
    rolling baseline average; alert on a surge, then roll the bucket forward.
    Networking (send_telegram) happens OUTSIDE the lock.
    """
    surges = []
    with _vol_lock:
        for sym in WATCHLIST:
            cur = _vol_current.get(sym, 0.0)
            hist = _vol_history[sym]
            if cur >= VOL_MIN_SHARES and len(hist) >= VOL_MIN_SAMPLES:
                avg = sum(hist) / len(hist)
                if avg > 0 and cur >= VOL_SURGE_MULT * avg:
                    surges.append((sym, cur, avg))
            hist.append(cur)
            _vol_current[sym] = 0.0

    for sym, cur, avg in surges:
        bucket = time.strftime("%Y%m%d%H%M")
        if not once("volsurge:" + sym + ":" + bucket):
            continue
        rvol = cur / avg if avg else 0
        send_telegram(
            "\U0001F50A <b>VOLUME SURGE</b>\n"
            + "<b>" + html.escape(sym) + "</b>  "
            + format(int(cur), ",") + " sh this minute = "
            + format(rvol, ".1f") + "x avg"
        )


# ----------------------------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------------------------
def main():
    log.info("Starting alert bot (dry_run=%s). Watchlist: %s", DRY_RUN, ", ".join(WATCHLIST))

    if DRY_RUN:
        # WebSocket needs a live connection, so just exercise the REST/RSS checks.
        for fn in (check_halts, check_movers, check_news):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                log.error("%s failed: %s", fn.__name__, e)
        log.info("Dry run complete.")
        return

    # Prime de-dup silently so a redeploy doesn't re-blast the existing backlog
    # (today's whole halt list + already-published news). Only NEW events after
    # this point will alert -- the same "from now on" behavior as the Make feeds.
    global _silent
    _silent = True
    for fn in (check_halts, check_news):
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            log.error("prime %s failed: %s", fn.__name__, e)
    _silent = False
    log.info("Primed de-dup on %d existing items; only new events will alert now.",
             len(_seen))

    # Start the volume-surge WebSocket in the background.
    t = threading.Thread(target=_ws_run, daemon=True)
    t.start()

    schedule = [
        (check_halts,         INTERVAL_HALTS),
        (check_movers,        INTERVAL_MOVERS),
        (check_news,          INTERVAL_NEWS),
        (check_volume_surge,  INTERVAL_VOLROLL),
    ]
    next_run = {fn.__name__: time.time() + interval for fn, interval in schedule}

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
