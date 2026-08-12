#!/usr/bin/env python3
"""
stock_alert_bot.py  (small-cap momentum edition)
------------------------------------------------
Real-time stock-alert bot that posts to a Telegram channel, focused on
SMALL-CAP / LOW-FLOAT / PENNY MOMENTUM names -- the stockmembers.com /
symbolalerts.com style -- instead of a fixed large-cap watchlist.

How it works:
  * A market-wide SCANNER (discover_movers) pulls the whole US market once a
    minute and keeps only small, cheap, fast-moving names (widest net:
    ~$0.10-$20, up >= X%, with real volume). Those become a DYNAMIC watchlist.
  * The dynamic watchlist then drives the real-time volume-surge WebSocket and
    the breaking-news check, so every alert is about an actual runner.

Alert types:
  1. SMALL-CAP RUNNER   -> market-wide scanner (Nasdaq screener, free)
  2. TRADING HALT       -> Nasdaq Trader Trade-Halt RSS (free, official)
  3. VOLUME SURGE       -> Finnhub trades WebSocket on the current runners (free)
  4. BREAKING NEWS      -> Finnhub /company-news on the current runners (free)

HYBRID DATA NOTE: discovery currently uses Nasdaq's free (unofficial) screener
endpoint -- $0, but it can rate-limit or change without notice, and it has no
true "float" figure (we approximate low float with shares-outstanding). To move
to a robust real-time feed later, replace ONLY discover_movers() with a
Polygon.io/Massive full-market snapshot call; everything downstream is unchanged.

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

# --- Small-cap scanner filters ("widest net": sub-$1 to ~$20) ---
PRICE_MIN      = 0.10      # ignore essentially-dead sub-penny junk below this
PRICE_MAX      = 20.00     # small-cap / penny ceiling
MIN_PERCENT    = 10.0      # only names up at least this % on the day
MIN_VOLUME     = 100000    # shares traded today (liquidity floor)
MIN_DOLLAR_VOL = 250000    # price * volume floor (kills illiquid sub-$1 traps)
TOP_N_ALERTS   = 15        # max NEW runner alerts per scan (paced sender caps rate)
MAX_WATCH      = 45        # max symbols kept on the free Finnhub WS (cap is 50)
LOWFLOAT_MAX_M = 50.0      # shares-out (millions) under which we tag "LOW FLOAT"

# Volume-surge thresholds (WebSocket trades)
VOL_BUCKET_SEC  = 60
VOL_HISTORY     = 20
VOL_MIN_SAMPLES = 5
VOL_SURGE_MULT  = 3.0
VOL_MIN_SHARES  = 5000

# Poll intervals (seconds).
INTERVAL_SCAN    = 60
INTERVAL_HALTS   = 20
INTERVAL_NEWS    = 180
INTERVAL_VOLROLL = VOL_BUCKET_SEC

NASDAQ_HALT_RSS = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
# Unofficial Nasdaq screener: whole US market in one call (symbol/price/%chg/vol/mcap).
NASDAQ_SCREENER = ("https://api.nasdaq.com/api/screener/stocks"
                   "?tableonly=true&limit=6000&offset=0&download=true")
NASDAQ_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}
FINNHUB_BASE = "https://finnhub.io/api/v1"
FINNHUB_WS   = "wss://ws.finnhub.io?token="

DRY_RUN = "--test" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("alertbot")

# De-dup memory so the same event isn't posted twice in a session.
_seen = set()

# Optional on-disk de-dup so a redeploy doesn't re-post the same runners.
# Persists across restarts IF a Railway Volume is mounted at /data; otherwise it
# falls back to a local (ephemeral) file and simply behaves as before.
SEEN_PATH = (_env("DEDUP_PATH")
             or ("/data/seen.json" if os.path.isdir("/data") else "seen_state.json"))


def _load_seen():
    """Load today's de-dup keys from disk (ignore the file if it's from another day)."""
    try:
        with open(SEEN_PATH) as f:
            blob = json.load(f)
    except (OSError, ValueError):
        return
    if blob.get("date") == time.strftime("%Y%m%d"):
        _seen.update(blob.get("seen", []))
        log.info("Loaded %d de-dup keys from %s", len(_seen), SEEN_PATH)
    else:
        log.info("De-dup file is from another day; starting fresh today.")


def _save_seen():
    """Atomically write the current de-dup set (with today's date) to disk."""
    try:
        tmp = SEEN_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"date": time.strftime("%Y%m%d"), "seen": sorted(_seen)}, f)
        os.replace(tmp, SEEN_PATH)
    except OSError as e:
        log.warning("Could not save de-dup file (%s): %s", SEEN_PATH, e)

# Global send pacing so a burst never trips Telegram's flood limit.
_send_lock = threading.Lock()
_last_send_ts = [0.0]
MIN_SEND_GAP = 3.1     # ~19 msgs/min: stays under Telegram's ~20/min channel cap
_silent = False

# Dynamic watchlist (the current small-cap runners) shared across threads.
_watch_lock = threading.Lock()
_watchlist = set()
_profile_cache = {}   # symbol -> {"shares_out_m": float|None}

# Volume-surge shared state (written by the WS thread, read by the rollup).
_vol_lock = threading.Lock()
_vol_current = defaultdict(float)
_vol_history = defaultdict(lambda: deque(maxlen=VOL_HISTORY))

# Live WebSocket handle + what it's currently subscribed to (managed dynamically).
_ws_app = [None]
_ws_subscribed = set()


# ----------------------------------------------------------------------
# TELEGRAM
# ----------------------------------------------------------------------
def send_telegram(text):
    """Send an HTML message to Telegram, paced so we never trip the flood limit."""
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


def shares_out_millions(sym):
    """Best-effort shares-outstanding in millions (our free 'low float' proxy)."""
    if sym in _profile_cache:
        return _profile_cache[sym].get("shares_out_m")
    val = None
    prof = finnhub_get("stock/profile2", {"symbol": sym})
    if isinstance(prof, dict):
        try:
            so = prof.get("shareOutstanding")
            val = float(so) if so else None   # Finnhub returns this in millions
        except (TypeError, ValueError):
            val = None
    _profile_cache[sym] = {"shares_out_m": val}
    return val


# ----------------------------------------------------------------------
# MARKET-WIDE DISCOVERY  (free Nasdaq screener)
# ----------------------------------------------------------------------
def _num(s):
    """Parse '$1.23', '12.34%', '1,234,567', '--' -> float or None."""
    if s is None:
        return None
    t = str(s).strip().replace("$", "").replace("%", "").replace(",", "")
    if t in ("", "--", "N/A", "NA", "UNCH"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def discover_movers():
    """
    Pull the whole US market and return small-cap momentum candidates:
    [{"symbol","price","pct","volume"}], already filtered and sorted by % desc.

    HYBRID UPGRADE POINT: to switch to a paid real-time feed later, replace the
    body of this function with a Polygon.io/Massive full-market snapshot call
    that returns the same list of dicts. Nothing else needs to change.
    """
    try:
        r = requests.get(NASDAQ_SCREENER, headers=NASDAQ_HEADERS, timeout=20)
    except requests.RequestException as e:
        log.error("Scanner fetch failed: %s", e)
        return []
    if r.status_code != 200:
        log.error("Scanner HTTP %s (Nasdaq screener may be throttling)", r.status_code)
        return []
    try:
        data = r.json().get("data") or {}
    except ValueError:
        log.error("Scanner returned non-JSON (%d bytes)", len(r.content))
        return []
    rows = data.get("rows")
    if rows is None:
        rows = (data.get("table") or {}).get("rows")
    if not rows:
        log.warning("Scanner returned no rows.")
        return []

    out = []
    for row in rows:
        sym = (row.get("symbol") or "").strip().upper()
        price = _num(row.get("lastsale"))
        pct = _num(row.get("pctchange"))
        vol = _num(row.get("volume"))
        if not sym or price is None or pct is None or vol is None:
            continue
        if "^" in sym or "/" in sym or "." in sym:   # skip warrants/units/odd tickers
            continue
        if price < PRICE_MIN or price > PRICE_MAX:
            continue
        if pct < MIN_PERCENT:
            continue
        if vol < MIN_VOLUME or (price * vol) < MIN_DOLLAR_VOL:
            continue
        out.append({"symbol": sym, "price": price, "pct": pct, "volume": vol})

    out.sort(key=lambda d: d["pct"], reverse=True)
    log.info("Scanner: %d rows -> %d small-cap movers matched", len(rows), len(out))
    return out


def _ws_sync(desired):
    """Subscribe/unsubscribe the live WS so it tracks exactly `desired` symbols."""
    ws = _ws_app[0]
    if ws is None:
        return
    for sym in desired - _ws_subscribed:
        try:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
            _ws_subscribed.add(sym)
        except Exception as e:  # noqa: BLE001
            log.warning("WS subscribe %s failed: %s", sym, e)
    for sym in _ws_subscribed - desired:
        try:
            ws.send(json.dumps({"type": "unsubscribe", "symbol": sym}))
        except Exception as e:  # noqa: BLE001
            log.warning("WS unsubscribe %s failed: %s", sym, e)
    _ws_subscribed.intersection_update(desired)


def scan_market():
    """Find small-cap runners, alert new ones, and refresh the dynamic watchlist."""
    movers = discover_movers()
    if not movers:
        return

    top = movers[:MAX_WATCH]
    desired = {m["symbol"] for m in top}

    # Refresh the shared watchlist that news + volume-surge use.
    with _watch_lock:
        _watchlist.clear()
        _watchlist.update(desired)
    _ws_sync(desired)

    day = time.strftime("%Y%m%d")
    alerted = 0
    for m in movers:
        if alerted >= TOP_N_ALERTS:
            break
        sym = m["symbol"]
        if not once("runner:" + sym + ":" + day):
            continue
        so = shares_out_millions(sym)
        tag = ""
        if so is not None and so <= LOWFLOAT_MAX_M:
            tag = "  •  \U0001F53B LOW FLOAT ~" + format(so, ".1f") + "M shares"
        arrow = "\U0001F680"  # rocket
        send_telegram(
            arrow + " <b>SMALL-CAP RUNNER</b>\n"
            + "<b>" + html.escape(sym) + "</b>  $" + format(m["price"], ",.2f")
            + "  (" + format(m["pct"], "+.1f") + "%)\n"
            + "Vol: " + format(int(m["volume"]), ",") + tag
        )
        alerted += 1


# ----------------------------------------------------------------------
# ALERT: TRADING HALTS  (Nasdaq Trader RSS, free -- already market-wide)
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
# ALERT: BREAKING NEWS  (Finnhub /company-news on the current runners)
# ----------------------------------------------------------------------
def check_news():
    with _watch_lock:
        syms = sorted(_watchlist)
    today = time.strftime("%Y-%m-%d")
    for sym in syms:
        items = finnhub_get("company-news", {"symbol": sym, "from": today, "to": today})
        if not isinstance(items, list):
            continue
        for n in items[:3]:
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
    _ws_app[0] = ws
    _ws_subscribed.clear()
    with _watch_lock:
        syms = set(_watchlist)
    for sym in syms:
        try:
            ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
            _ws_subscribed.add(sym)
        except Exception as e:  # noqa: BLE001
            log.error("WS subscribe failed for %s: %s", sym, e)
    log.info("Volume WebSocket connected; tracking %d runners", len(_ws_subscribed))


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
        _ws_app[0] = None
        time.sleep(5)  # reconnect backoff


def check_volume_surge():
    """Compare each runner's just-finished volume bucket to its rolling baseline."""
    with _watch_lock:
        syms = list(_watchlist)
    surges = []
    with _vol_lock:
        for sym in syms:
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
    log.info("Starting small-cap alert bot (dry_run=%s). Filters: $%.2f-$%.2f, "
             ">=%.0f%%, vol>=%s", DRY_RUN, PRICE_MIN, PRICE_MAX, MIN_PERCENT,
             format(MIN_VOLUME, ","))

    if DRY_RUN:
        for fn in (scan_market, check_halts, check_news):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                log.error("%s failed: %s", fn.__name__, e)
        log.info("Dry run complete.")
        return

    # Restore today's de-dup memory from disk so a redeploy doesn't re-post the
    # runners/news/halts already sent earlier today (needs a Railway Volume at
    # /data to survive redeploys; otherwise this is a no-op).
    _load_seen()

    # Prime the halt feed silently so a redeploy doesn't re-blast today's
    # existing halt backlog. (The scanner is capped per-scan, so it isn't primed
    # -- on start it will show whatever is currently running, then only new ones.)
    global _silent
    _silent = True
    try:
        check_halts()
    except Exception as e:  # noqa: BLE001
        log.error("prime check_halts failed: %s", e)
    _silent = False
    log.info("Primed %d existing halt items; only new events will alert now.", len(_seen))

    # Start the volume-surge WebSocket in the background.
    t = threading.Thread(target=_ws_run, daemon=True)
    t.start()

    schedule = [
        (scan_market,        INTERVAL_SCAN),
        (check_halts,        INTERVAL_HALTS),
        (check_news,         INTERVAL_NEWS),
        (check_volume_surge, INTERVAL_VOLROLL),
    ]
    # Run the first market scan almost immediately so the watchlist populates.
    next_run = {fn.__name__: time.time() + (2 if fn is scan_market else interval)
                for fn, interval in schedule}
    last_saved_n = len(_seen)

    while True:
        now = time.time()
        for fn, interval in schedule:
            if now >= next_run[fn.__name__]:
                try:
                    fn()
                except Exception as e:  # noqa: BLE001 - never let one feed kill the loop
                    log.error("%s failed: %s", fn.__name__, e)
                next_run[fn.__name__] = now + interval
        # Persist de-dup only when something new was actually sent.
        if len(_seen) != last_saved_n:
            _save_seen()
            last_saved_n = len(_seen)
        time.sleep(1)


if __name__ == "__main__":
    main()
