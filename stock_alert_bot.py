#!/usr/bin/env python3
"""
stock_alert_bot.py  (small-cap momentum edition -- extended hours + news)
-------------------------------------------------------------------------
Real-time stock-alert bot that posts to a Telegram channel, focused on
SMALL-CAP / LOW-FLOAT / PENNY MOMENTUM names (stockmembers.com /
symbolalerts.com style).

WHY THIS VERSION EXISTS
-----------------------
v1 only saw REGULAR-SESSION data and only checked news for symbols the price
scanner had already flagged. That made it blind to exactly the alerts the
competitors send:
  * OFAL was +291% in PREMARKET (04:00-09:30 ET) -- the regular-session
    screener reports ~0% change then, so it never crossed the filter.
  * RMCF moved on an SEC filing / press release. It was not a big regular-hours
    mover, so it never entered the watchlist, so its news was never fetched.

This version fixes both:
  1. EXTENDED HOURS: polls Nasdaq's free per-symbol extended-trading endpoint
     during premarket and after-hours, alerting on big gaps with real volume.
  2. NEWS FIRST: scans news market-wide, then alerts on any small-cap ticker.
     News tickers also become premarket polling candidates (the catalyst leads
     the move, so this is what finds runners before they show up on a screener).

Alert types:
  1. SMALL-CAP RUNNER    -> regular-session market-wide scanner (Nasdaq, free)
  2. PREMARKET / AFTER-HOURS RUNNER -> Nasdaq extended-trading (free)
  3. NEWS / CATALYST     -> market-wide news + PR wires, filtered to small caps
  4. TRADING HALT        -> Nasdaq Trader Trade-Halt RSS (free, official)
  5. VOLUME SURGE        -> Finnhub trades WebSocket on current runners (free)

HYBRID DATA NOTE: every source here is free. The Nasdaq endpoints are
unofficial and can throttle or change without notice; each failure is logged
and skipped rather than crashing. "Low float" is approximated with
shares-outstanding (true float is premium data everywhere).

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
import re
import sys
import json
import time
import html
import logging
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:  # noqa: BLE001 - fall back to UTC-ish if tzdata missing
    ET = None

import requests

try:
    import feedparser  # halt RSS + PR wire feeds
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

# --- Small-cap universe / regular-session filters ---
PRICE_MIN      = 0.10
PRICE_MAX      = 20.00
MAX_MARKET_CAP = 2_000_000_000     # ignore anything bigger than ~$2B
MIN_PERCENT    = 10.0              # regular-hours move threshold
MIN_VOLUME     = 100000
MIN_DOLLAR_VOL = 250000
TOP_N_ALERTS   = 15
MAX_WATCH      = 45                # free Finnhub WS cap is 50
LOWFLOAT_MAX_M = 50.0              # shares-out (millions) -> "LOW FLOAT" tag

# --- Extended-hours (premarket / after-hours) filters ---
PM_MIN_PERCENT   = 20.0            # bigger threshold: extended moves are wilder
PM_MIN_VOLUME    = 50000           # extended-session share volume floor
EXT_WORKERS      = 20              # concurrent extended-quote fetches
                                   # (measured: ~43ms/symbol, 0 failures at 12)
MAX_EXT_ALERTS   = 20              # alerts per sweep (biggest movers first)

# Volume-surge thresholds (WebSocket trades)
VOL_BUCKET_SEC  = 60
VOL_HISTORY     = 20
VOL_MIN_SAMPLES = 5
VOL_SURGE_MULT  = 3.0
VOL_MIN_SHARES  = 5000

# Poll intervals (seconds)
INTERVAL_SCAN     = 60
INTERVAL_EXTENDED = 120
INTERVAL_UNIVERSE = 600
INTERVAL_HALTS    = 20
INTERVAL_NEWS     = 120
INTERVAL_VOLROLL  = VOL_BUCKET_SEC

NASDAQ_HALT_RSS = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
NASDAQ_SCREENER = ("https://api.nasdaq.com/api/screener/stocks"
                   "?tableonly=true&limit=6000&offset=0&download=true")
NASDAQ_EXTENDED = ("https://api.nasdaq.com/api/quote/{sym}/extended-trading"
                   "?assetclass=stocks&markettype={mt}&time=1")
NASDAQ_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}
NASDAQ_QUOTE    = "https://api.nasdaq.com/api/quote/{sym}/info?assetclass=stocks"
FINNHUB_BASE = "https://finnhub.io/api/v1"
FINNHUB_WS   = "wss://ws.finnhub.io?token="

# Free PR-wire RSS feeds (no key). These carry the catalysts that move small caps.
PR_FEEDS = [
    "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire-News-about-Public-Companies",
    "https://www.prnewswire.com/rss/news-releases-list.rss",
]

DRY_RUN = "--test" in sys.argv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("alertbot")

# De-dup memory so the same event isn't posted twice.
_seen = set()

# Optional on-disk de-dup so a redeploy doesn't re-post the same alerts.
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

# Small-cap universe: symbol -> {"close": float, "mcap": float, "name": str, "vol": float}
_universe = {}
_universe_lock = threading.Lock()

# Tickers with a catalyst today (news/filing/halt) -> priority premarket candidates.
_catalyst = set()

# Dynamic watchlist (current runners) shared across threads.
_watch_lock = threading.Lock()
_watchlist = set()
_profile_cache = {}

# Volume-surge shared state (written by the WS thread, read by the rollup).
_vol_lock = threading.Lock()
_vol_current = defaultdict(float)
_vol_history = defaultdict(lambda: deque(maxlen=VOL_HISTORY))
_vol_lastpx = {}          # symbol -> last real-time trade price from the WS

_ws_app = [None]
_ws_subscribed = set()


# ----------------------------------------------------------------------
# SESSION CLOCK (US/Eastern)
# ----------------------------------------------------------------------
def now_et():
    return datetime.now(ET) if ET else datetime.utcnow()


def session():
    """Return 'pre', 'regular', 'post', or 'closed' for the current ET time."""
    n = now_et()
    if n.weekday() >= 5:                 # Sat/Sun
        return "closed"
    mins = n.hour * 60 + n.minute
    if 4 * 60 <= mins < 9 * 60 + 30:
        return "pre"
    if 9 * 60 + 30 <= mins < 16 * 60:
        return "regular"
    if 16 * 60 <= mins < 20 * 60:
        return "post"
    return "closed"


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
# HELPERS
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


def finnhub_get(path, params=None):
    if not FINNHUB_API_KEY:
        return None
    params = dict(params or {})
    params["token"] = FINNHUB_API_KEY
    try:
        r = requests.get(FINNHUB_BASE + "/" + path, params=params, timeout=15)
        if r.status_code == 429:
            log.warning("Finnhub rate limit on %s; backing off", path)
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
    """Best-effort shares-outstanding in millions (free 'low float' proxy)."""
    if sym in _profile_cache:
        return _profile_cache[sym].get("shares_out_m")
    val = None
    prof = finnhub_get("stock/profile2", {"symbol": sym})
    if isinstance(prof, dict):
        try:
            so = prof.get("shareOutstanding")
            val = float(so) if so else None
        except (TypeError, ValueError):
            val = None
    _profile_cache[sym] = {"shares_out_m": val}
    return val


def current_price(sym):
    """Best-effort live price: WS last trade -> Nasdaq real-time quote -> prior close."""
    with _vol_lock:
        px = _vol_lastpx.get(sym)
    if px:
        return px
    try:
        r = requests.get(NASDAQ_QUOTE.format(sym=sym), headers=NASDAQ_HEADERS, timeout=8)
        if r.status_code == 200:
            pdata = ((r.json().get("data") or {}).get("primaryData") or {})
            px = _num(pdata.get("lastSalePrice"))
            if px:
                return px
    except (requests.RequestException, ValueError):
        pass
    with _universe_lock:
        return (_universe.get(sym) or {}).get("close")


def lowfloat_tag(sym):
    so = shares_out_millions(sym)
    if so is not None and so <= LOWFLOAT_MAX_M:
        return "  •  \U0001F53B LOW FLOAT ~" + format(so, ".1f") + "M sh"
    return ""


# ----------------------------------------------------------------------
# UNIVERSE  (whole US market -> small-cap subset)
# ----------------------------------------------------------------------
def refresh_universe():
    """Pull the full market once and cache the small-cap subset with prior close."""
    try:
        r = requests.get(NASDAQ_SCREENER, headers=NASDAQ_HEADERS, timeout=25)
    except requests.RequestException as e:
        log.error("Universe fetch failed: %s", e)
        return
    if r.status_code != 200:
        log.error("Universe HTTP %s (Nasdaq may be throttling)", r.status_code)
        return
    try:
        data = r.json().get("data") or {}
    except ValueError:
        log.error("Universe returned non-JSON")
        return
    rows = data.get("rows") or (data.get("table") or {}).get("rows") or []
    if not rows:
        log.warning("Universe returned no rows.")
        return

    uni = {}
    for row in rows:
        sym = (row.get("symbol") or "").strip().upper()
        price = _num(row.get("lastsale"))
        if not sym or price is None:
            continue
        if "^" in sym or "/" in sym or "." in sym:
            continue
        if price < PRICE_MIN or price > PRICE_MAX:
            continue
        mcap = _num(row.get("marketCap"))
        if mcap is not None and mcap > MAX_MARKET_CAP:
            continue
        uni[sym] = {
            "close": price,
            "mcap": mcap,
            "name": (row.get("name") or "").strip(),
            "vol": _num(row.get("volume")) or 0.0,
        }
    with _universe_lock:
        _universe.clear()
        _universe.update(uni)
    log.info("Universe: %d rows -> %d small-cap symbols", len(rows), len(uni))


# ----------------------------------------------------------------------
# ALERT 1: REGULAR-SESSION RUNNERS
# ----------------------------------------------------------------------
def discover_movers():
    """Regular-hours movers from the market-wide screener (sorted by % desc)."""
    try:
        r = requests.get(NASDAQ_SCREENER, headers=NASDAQ_HEADERS, timeout=25)
    except requests.RequestException as e:
        log.error("Scanner fetch failed: %s", e)
        return []
    if r.status_code != 200:
        log.error("Scanner HTTP %s (Nasdaq screener may be throttling)", r.status_code)
        return []
    try:
        data = r.json().get("data") or {}
    except ValueError:
        log.error("Scanner returned non-JSON")
        return []
    rows = data.get("rows") or (data.get("table") or {}).get("rows") or []
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
        if "^" in sym or "/" in sym or "." in sym:
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
    """Regular hours only: find runners, alert new ones, refresh the watchlist."""
    if session() != "regular":
        return
    movers = discover_movers()
    if not movers:
        return

    desired = {m["symbol"] for m in movers[:MAX_WATCH]}
    with _watch_lock:
        _watchlist.clear()
        _watchlist.update(desired)
    _ws_sync(desired)

    day = now_et().strftime("%Y%m%d")
    alerted = 0
    for m in movers:
        if alerted >= TOP_N_ALERTS:
            break
        sym = m["symbol"]
        if not once("runner:" + sym + ":" + day):
            continue
        send_telegram(
            "\U0001F680 <b>SMALL-CAP RUNNER</b>\n"
            + "<b>" + html.escape(sym) + "</b>  $" + format(m["price"], ",.2f")
            + "  (" + format(m["pct"], "+.1f") + "%)\n"
            + "Vol: " + format(int(m["volume"]), ",") + lowfloat_tag(sym)
        )
        alerted += 1


# ----------------------------------------------------------------------
# ALERT 2: PREMARKET / AFTER-HOURS RUNNERS  (Nasdaq extended-trading, free)
# ----------------------------------------------------------------------
_PCT_RE = re.compile(r"\(([+-]?[\d.]+)\s*%\)")
_PRICE_RE = re.compile(r"\$([\d.]+)")


def parse_extended(payload):
    """
    Parse Nasdaq's extended-trading JSON.
    Returns {"price","pct","volume","high"} or None.
    """
    try:
        d = (payload or {}).get("data") or {}
        info = d.get("infoTable") or {}
        rows = info.get("rows") or []
        if not rows:
            return None
        row = rows[0]
        cons = str(row.get("consolidated") or "")
        pm = _PRICE_RE.search(cons)
        pc = _PCT_RE.search(cons)
        if not pm or not pc:
            return None
        hm = _PRICE_RE.search(str(row.get("highPrice") or ""))
        return {
            "price": float(pm.group(1)),
            "pct": float(pc.group(1)),
            "volume": _num(row.get("volume")) or 0.0,
            "high": float(hm.group(1)) if hm else None,
        }
    except (AttributeError, TypeError, ValueError):
        return None


def fetch_extended(sym, markettype):
    """Fetch one symbol's extended-hours quote. Returns parsed dict or None."""
    url = NASDAQ_EXTENDED.format(sym=sym, mt=markettype)
    try:
        r = requests.get(url, headers=NASDAQ_HEADERS, timeout=12)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        return None
    try:
        return parse_extended(r.json())
    except ValueError:
        return None


def build_candidates():
    """
    EVERY small-cap symbol -- ordered by priority so that if a sweep gets
    throttled part-way, the names most likely to run are already covered:
      1. today's catalyst names (news / filings / halts)
      2. names already flagged as runners
      3. everything else, most-liquid first (prior-session dollar volume)

    NOTE: this deliberately returns the FULL universe. An earlier version capped
    this at 200 symbols by prior-day volume, which silently missed real runners
    (e.g. WETO +177% premarket on only 88,970 shares the prior session).
    """
    with _universe_lock:
        uni = dict(_universe)
    if not uni:
        return []
    ordered, seen = [], set()
    for group in (sorted(_catalyst), sorted(_watchlist)):
        for s in group:
            if s in uni and s not in seen:
                ordered.append(s)
                seen.add(s)
    rest = sorted(uni.items(),
                  key=lambda kv: (kv[1].get("vol") or 0) * (kv[1].get("close") or 0),
                  reverse=True)
    for s, _info in rest:
        if s not in seen:
            ordered.append(s)
            seen.add(s)
    return ordered


def scan_extended():
    """Premarket / after-hours gap scanner across the WHOLE small-cap universe."""
    sess = session()
    if sess not in ("pre", "post"):
        return
    markettype = "pre" if sess == "pre" else "post"
    cands = build_candidates()
    if not cands:
        return

    label = "PREMARKET" if sess == "pre" else "AFTER-HOURS"
    t0 = time.time()
    hits = []
    fails = 0

    def probe(sym):
        return sym, fetch_extended(sym, markettype)

    with ThreadPoolExecutor(max_workers=EXT_WORKERS) as pool:
        for sym, q in pool.map(probe, cands):
            if not q:
                fails += 1
                continue
            price, pct, vol = q["price"], q["pct"], q["volume"]
            if price < PRICE_MIN or price > PRICE_MAX:
                continue
            if pct < PM_MIN_PERCENT or vol < PM_MIN_VOLUME:
                continue
            hits.append((pct, sym, price, vol, q.get("high")))

    hits.sort(key=lambda h: h[0], reverse=True)   # biggest movers alert first
    day = now_et().strftime("%Y%m%d")
    sent = 0
    for pct, sym, price, vol, high in hits:
        if sent >= MAX_EXT_ALERTS:
            break
        # Re-alert only on a materially bigger move (every extra 50%).
        tier = int(pct // 50)
        if not once("ext:" + markettype + ":" + sym + ":" + day + ":" + str(tier)):
            continue
        extra = ("\n" + label.title() + " High: $" + format(high, ",.2f")) if high else ""
        send_telegram(
            "\U0001F680 <b>" + label + " RUNNER</b>\n"
            + "<b>" + html.escape(sym) + "</b>  $" + format(price, ",.2f")
            + "  (" + format(pct, "+.1f") + "%)" + extra + "\n"
            + label.title() + " Vol: " + format(int(vol), ",") + lowfloat_tag(sym)
        )
        sent += 1
    log.info("%s sweep: %d symbols in %.0fs -> %d qualifying, %d alerts, %d fetch fails",
             label, len(cands), time.time() - t0, len(hits), sent, fails)


def _extended_loop():
    """Run the extended-hours sweep on its own thread.

    A full sweep takes ~1-2 minutes, so it must not block the main scheduler
    (halts run every 20s and would otherwise be delayed behind it).
    """
    while True:
        started = time.time()
        try:
            scan_extended()
        except Exception as e:  # noqa: BLE001
            log.error("scan_extended failed: %s", e)
        # Cycle-aware: sleep only the remainder so sweeps land on a steady beat.
        time.sleep(max(10, INTERVAL_EXTENDED - (time.time() - started)))

# ----------------------------------------------------------------------
# ALERT 3: MARKET-WIDE NEWS / CATALYSTS
# ----------------------------------------------------------------------
_TICKER_RE = re.compile(r"\b([A-Z]{1,5})\b")


def tickers_in(text):
    """Uppercase tokens in text that are real symbols in our small-cap universe."""
    if not text:
        return set()
    with _universe_lock:
        uni = _universe
        return {t for t in _TICKER_RE.findall(text.upper()) if t in uni}


def _news_alert(sym, headline, source, url, tag="NEWS"):
    with _universe_lock:
        info = _universe.get(sym) or {}
    px = info.get("close")
    price_str = ("  $" + format(px, ",.2f")) if px else ""
    send_telegram(
        "\U0001F4F0 <b>" + tag + "</b>  <b>" + html.escape(sym) + "</b>" + price_str + "\n"
        + html.escape(headline[:250])
    )


def check_market_news():
    """
    Scan news MARKET-WIDE (not just the watchlist) and alert on any small-cap
    ticker. Also records those tickers as premarket polling candidates.
    """
    found = 0

    # --- Source A: Finnhub general market news (free tier, has `related` tickers)
    items = finnhub_get("news", {"category": "general"})
    if isinstance(items, list):
        for n in items[:60]:
            url = n.get("url") or ""
            nid = "news:" + str(n.get("id") or url)
            headline = str(n.get("headline") or "")
            related = str(n.get("related") or "")
            syms = tickers_in(related) or tickers_in(headline)
            if not syms:
                continue
            for sym in list(syms)[:2]:
                _catalyst.add(sym)
                if once(nid + ":" + sym):
                    _news_alert(sym, headline, str(n.get("source") or ""), url)
                    found += 1

    # --- Source B: free PR wires (where small-cap catalysts break first)
    if feedparser is not None:
        for feed_url in PR_FEEDS:
            try:
                feed = feedparser.parse(feed_url)
            except Exception as e:  # noqa: BLE001
                log.warning("PR feed error (%s): %s", feed_url, e)
                continue
            for entry in getattr(feed, "entries", [])[:40]:
                title = entry.get("title", "")
                link = entry.get("link", "")
                syms = tickers_in(title)
                if not syms:
                    continue
                for sym in list(syms)[:2]:
                    _catalyst.add(sym)
                    key = "pr:" + (entry.get("id") or link or title)[:120] + ":" + sym
                    if once(key):
                        _news_alert(sym, title, "PR Wire", link, tag="CATALYST")
                        found += 1

    if found:
        log.info("News scan: %d new catalyst alerts (%d tickers tracked)",
                 found, len(_catalyst))


# ----------------------------------------------------------------------
# ALERT 4: TRADING HALTS
# ----------------------------------------------------------------------
def check_halts():
    if feedparser is None:
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
        if _silent:
            continue        # priming a redeploy: record the id, don't fetch or send
        # The feed's <title> is just the ticker (e.g. "NIPG"). Its <description>
        # is a raw HTML table -- deliberately ignored so it never hits the channel.
        sym = (entry.get("ndaq_issuesymbol")
               or entry.get("title", "")).strip().upper()
        if not sym or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,7}", sym):
            continue
        _catalyst.add(sym)            # halted names are prime premarket candidates
        px = current_price(sym)
        price_str = ("  $" + format(px, ",.2f")) if px else ""
        send_telegram(
            "\U0001F6A8 <b>TRADING HALT</b>\n"
            + "<b>" + html.escape(sym) + "</b>" + price_str
        )


# ----------------------------------------------------------------------
# ALERT 5: VOLUME SURGE  (Finnhub trades WebSocket, real-time)
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
                px = t.get("p")
                if px:
                    _vol_lastpx[sym] = float(px)


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
        time.sleep(5)


def check_volume_surge():
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
                    surges.append((sym, _vol_lastpx.get(sym)))
            hist.append(cur)
            _vol_current[sym] = 0.0

    for sym, px in surges:
        bucket = now_et().strftime("%Y%m%d%H%M")
        if not once("volsurge:" + sym + ":" + bucket):
            continue
        if px is None:                      # WS price missing -> fall back to close
            with _universe_lock:
                px = (_universe.get(sym) or {}).get("close")
        price_str = ("  $" + format(px, ",.2f")) if px else ""
        send_telegram(
            "\U0001F50A <b>VOLUME SURGE</b>\n"
            + "<b>" + html.escape(sym) + "</b>" + price_str
        )


# ----------------------------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------------------------
def main():
    log.info("Starting small-cap alert bot (dry_run=%s). Session=%s. "
             "Regular: $%.2f-$%.2f >=%.0f%% | Extended: >=%.0f%% vol>=%s",
             DRY_RUN, session(), PRICE_MIN, PRICE_MAX, MIN_PERCENT,
             PM_MIN_PERCENT, format(PM_MIN_VOLUME, ","))

    refresh_universe()

    if DRY_RUN:
        for fn in (scan_market, scan_extended, check_market_news, check_halts):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                log.error("%s failed: %s", fn.__name__, e)
        log.info("Dry run complete.")
        return

    _load_seen()

    # Prime the halt feed silently so a redeploy doesn't re-blast today's backlog.
    global _silent
    _silent = True
    try:
        check_halts()
    except Exception as e:  # noqa: BLE001
        log.error("prime check_halts failed: %s", e)
    _silent = False
    log.info("Primed %d existing items; only new events will alert now.", len(_seen))

    t = threading.Thread(target=_ws_run, daemon=True)
    t.start()
    # Full-universe extended sweep runs on its own thread (it takes ~1-2 min).
    threading.Thread(target=_extended_loop, daemon=True).start()

    schedule = [
        (refresh_universe,   INTERVAL_UNIVERSE),
        (scan_market,        INTERVAL_SCAN),
        (check_market_news,  INTERVAL_NEWS),
        (check_halts,        INTERVAL_HALTS),
        (check_volume_surge, INTERVAL_VOLROLL),
    ]
    # Kick off the scanners almost immediately so alerts start flowing.
    soon = (scan_market, check_market_news)
    next_run = {fn.__name__: time.time() + (3 if fn in soon else interval)
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
                next_run[fn.__name__] = time.time() + interval
        if len(_seen) != last_saved_n:
            _save_seen()
            last_saved_n = len(_seen)
        time.sleep(1)


if __name__ == "__main__":
    main()
