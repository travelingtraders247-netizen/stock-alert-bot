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
import queue
import logging
import logging.handlers
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:  # noqa: BLE001 - fall back to UTC-ish if tzdata missing
    ET = None

import requests
from requests.adapters import HTTPAdapter

# Shared, pooled HTTP session. The premarket sweep makes thousands of requests
# per pass; with a bare requests.get() each one opened a brand-new TCP socket,
# which exhausted file descriptors and silently wedged the whole process
# mid-session (it died twice this way). A pooled Session reuses connections and
# bounds the socket count.
_http = requests.Session()
_adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
_http.mount("https://", _adapter)
_http.mount("http://", _adapter)

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
MAX_MARKET_CAP = 500_000_000       # FLEET-WIDE cap: no alert of any type fires for a
                                   # company bigger than this. Nasdaq reports
                                   # marketCap in dollars (Ford = 56,499,026,300).
                                   # Symbols with no cap data are allowed through.
MIN_PERCENT    = 10.0              # regular-hours move threshold

# --- Unusual-volume (relative volume) detection ---
# A gain-only scanner is blind to accumulation days. AMIX traded 132x its normal
# volume on 08/24 while closing only +1.9% -- invisible to us -- and ran +86%
# intraday the very next session. This catches that setup.
RVOL_MIN         = 5.0             # today's volume vs prior median
RVOL_MIN_SHARES  = 750_000         # ignore thin names spiking off a tiny base
RVOL_MIN_DOLLAR  = 1_000_000
RVOL_LOOKUPS_MAX = 15              # new baseline fetches per scan (rate control)
MIN_VOLUME     = 100000
MIN_DOLLAR_VOL = 250000
TOP_N_ALERTS   = 15

# Runner confirmation. A >=10% move alone is not enough: it must carry real
# volume and have actually travelled intraday rather than gapping and stalling.
RUNNER_RVOL_MIN      = 2.0   # day volume vs median prior daily volume
RUNNER_MIN_FROM_OPEN = 7.0   # % gained since today's open
RUNNER_CTX_LOOKUPS   = 20    # baseline fetches per scan (cached per day)
MAX_WATCH      = 45                # free Finnhub WS cap is 50
LOWFLOAT_MAX_M = 50.0              # shares-out (millions) -> "LOW FLOAT" tag

# --- First-day listings (IPO / direct listing / de-SPAC) ---
# No prior close exists, so every %-change filter is blind to them. Measured
# on the session's own low-to-high range instead.
NEW_LISTING_MIN_RANGE = 20.0       # % from session low
NEW_LISTING_MIN_VOL   = 250_000
NEW_LISTING_MAX_POLL  = 40         # candidates polled per pass

# --- Extended-hours (premarket / after-hours) filters ---
PM_MIN_PERCENT   = 20.0            # bigger threshold: extended moves are wilder
PM_MIN_VOLUME    = 50000           # extended-session share volume floor
EXT_WORKERS      = 10               # concurrent extended fetches (trial-plan safe)
                                   # (measured: ~43ms/symbol, 0 failures at 12)
MAX_EXT_ALERTS   = 20              # alerts per sweep (biggest movers first)
EXT_RVOL_MIN        = 2.0          # extended volume vs a normal FULL day
EXT_RVOL_MIN_SHARES = 400_000      # absolute floor before we bother looking up
EXT_RVOL_LOOKUPS    = 12           # baseline fetches per sweep
EXT_TIMEOUT         = 8            # per-symbol extended fetch timeout
EXT_PRIORITY_PCT    = 10.0         # |today move| that earns a front-of-queue probe
EXT_PRIORITY_VOL    = 300_000      # ...paired with real turnover
EXT_TAIL_CHUNK      = 700          # long-tail symbols per sweep (rotates)

# Volume-surge thresholds (WebSocket trades)
VOL_BUCKET_SEC  = 60
VOL_HISTORY     = 20
VOL_MIN_SAMPLES = 5
VOL_SURGE_MULT  = 3.0
VOL_MIN_SHARES  = 5000

# Poll intervals (seconds)
INTERVAL_SCAN     = 60
INTERVAL_EXTENDED = 300
INTERVAL_UNIVERSE = 600
INTERVAL_HALTS    = 20
INTERVAL_NEWS     = 120
INTERVAL_MOVER_NEWS = 300
INTERVAL_NEWLIST  = 120
INTERVAL_VOLROLL  = VOL_BUCKET_SEC

NASDAQ_HALT_RSS = "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
NASDAQ_SCREENER = ("https://api.nasdaq.com/api/screener/stocks"
                   "?tableonly=true&limit=6000&offset=0&download=true")
NASDAQ_EXTENDED = ("https://api.nasdaq.com/api/quote/{sym}/extended-trading"
                   "?assetclass=stocks&markettype={mt}&time=1")
NASDAQ_IPO      = "https://api.nasdaq.com/api/ipo/calendar?date={ym}"
NASDAQ_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}
NASDAQ_QUOTE    = "https://api.nasdaq.com/api/quote/{sym}/info?assetclass=stocks"
NASDAQ_HIST     = ("https://api.nasdaq.com/api/quote/{sym}/historical"
                   "?assetclass=stocks&fromdate={frm}&todate={to}&limit=15")
FINNHUB_BASE = "https://finnhub.io/api/v1"
FINNHUB_WS   = "wss://ws.finnhub.io?token="

# Free PR-wire RSS feeds (no key). These carry the catalysts that move small caps.
PR_FEEDS = [
    "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire-News-about-Public-Companies",
    "https://www.prnewswire.com/rss/news-releases-list.rss",
    "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
]
# NOTE: each wire feed only exposes ~20 recent items and is mostly noise
# (European regulatory filings, consumer PR), so a small-cap release can scroll
# off before we see it. ACCESSWIRE's public feed is dead and Business Wire's
# broad feeds are a paid media-partner product. check_mover_news() closes that
# gap from the other direction.

# Nasdaq / UTP halt reason codes -> plain English. Unmapped codes show as-is.
# The distinction matters: LUDP/T5 usually resume quickly, T1 (news pending)
# and the H-series (compliance) are far more serious.
HALT_REASONS = {
    "T1": "News Pending",
    "T2": "News Released",
    "T5": "Volatility Pause",
    "T6": "Extraordinary Market Activity",
    "T7": "Correction of a Transaction",
    "T8": "ETF Component Halt",
    "T12": "Additional Information Requested",
    "H4": "Non-Compliance",
    "H9": "Filings Not Current",
    "H10": "SEC Trading Suspension",
    "H11": "Regulatory Concern",
    "IPO1": "IPO Not Yet Trading",
    "IPOQ": "IPO Quote Period",
    "LUDP": "Volatility Pause (Limit Up/Down)",
    "LUDS": "Volatility Pause - Straddle",
    "MWC0": "Market-Wide Circuit Breaker",
    "MWC1": "Market-Wide Circuit Breaker L1",
    "MWC2": "Market-Wide Circuit Breaker L2",
    "MWC3": "Market-Wide Circuit Breaker L3",
    "M": "Volatility Pause",
    "D": "News Dissemination",
    "R1": "New Issue Available",
    "R4": "Qualifications Issues Resolved",
    "R9": "Filings Complete",
    "C3": "Issuer News Not Forthcoming",
    "C4": "Qualifications Halt Ended",
    "C9": "Filings Complete",
    "C11": "Trade Correction",
}

# Catalyst/news throttle. Article-level de-dup can't stop the same story arriving
# from Finnhub + GlobeNewswire + PRNewswire under three different ids, so cap how
# often any one ticker can produce a catalyst alert in a rolling window.
NEWS_MAX_PER_24H = 2
NEWS_WINDOW_SEC  = 86400

# --- SEC EDGAR filings -------------------------------------------------------
# A filing on its own is NOT an alert. Thousands are published every day and most
# move nothing. A queued filing only becomes an alert once the tape confirms it:
# the ticker has to be up meaningfully, or turning over unusual volume, or
# already flagged by another alert today. Unconfirmed filings expire quietly.
EDGAR_MIN_PCT      = 7.0           # move that counts as confirmation on its own
EDGAR_MIN_RVOL     = 2.0           # ...or this much of a normal day's volume
EDGAR_PENDING_SEC  = 21600         # hold an unconfirmed filing 6h, then drop it
EDGAR_MAX_PENDING  = 300
EDGAR_RVOL_LOOKUPS = 10            # baseline fetches per confirmation pass
EDGAR_TIMEOUT      = 30            # browse-edgar is slow; 20s timed out
INTERVAL_EDGAR     = 300
INTERVAL_EDGAR_OK  = 120

# --- Top-10 gainers board ----------------------------------------------------
# A leaderboard of the biggest percentage gainers, run separately for each
# session (premarket / regular / after-hours) so each board reflects that
# session's own move. The full board posts once when a session opens; after
# that only NEW entrants are posted, so a name that stays on the board is never
# repeated within the same session.
BOARD_N            = 10
BOARD_PRICE_MIN    = 0.50
BOARD_PRICE_MAX    = 25.00
BOARD_MIN_DOLLAR   = 250_000       # session turnover needed to be tradeable
BOARD_POOL_MAX     = 60            # rows kept from each screener pass
BOARD_MIN_OPEN     = 3             # do not open a board on one lonely name
INTERVAL_BOARD     = 900           # re-rank every 15 minutes
WEBULL_QUOTE_MAX   = 40            # tickers per batch quote call

# Webull publishes a market-wide, real-time PREMARKET and AFTER-HOURS gainers
# ranking in a single call. Nasdaq has no free equivalent -- its screener keeps
# reporting the last REGULAR print until the next open -- which is why our
# extended boards could only ever rank what our own sweep happened to poll.
# Unofficial endpoint, so every failure is treated as normal and we fall back
# to the sweep.
WEBULL_RANK_URL = ("https://quotes-gw.webullfintech.com/api/wlas/ranking/"
                   "topGainers?regionId=6&rankType={rt}&pageIndex=1&pageSize={n}")
# The ranking gives price and percent but not extended VOLUME, and the liquidity
# floor needs it. This batch quote endpoint returns pPrice / pChRatio / pVolume
# for many tickers at once, so confirming the whole board costs one request.
WEBULL_QUOTE_URL = ("https://quotes-gw.webullfintech.com/api/bgw/quote/realtime"
                    "?ids={ids}&includeSecu=1&delay=0&more=1")
WEBULL_PAGE = 60
WEBULL_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
}

# Never block the scheduler for more than this on a Telegram rate-limit, and emit
# a heartbeat so a silent stall is obvious in the logs.
MAX_TG_SLEEP     = 60
HEARTBEAT_SEC    = 900

# Watchdog. This process has silently wedged several times -- alive enough that
# Railway still reported "ACTIVE", but the scheduler loop stopped ticking and no
# alerts went out for hours. Rather than keep guessing at the cause, fail fast:
# if the main loop stops ticking, kill the process so Railway's "restart on
# failure" policy brings it straight back.
WATCHDOG_TIMEOUT = 600
SAVE_MIN_GAP     = 60      # min seconds between de-dup writes to the volume
_last_tick = [time.time()]

DRY_RUN = "--test" in sys.argv

# --- Non-blocking logging ---------------------------------------------------
# Railway reads our stdout through a pipe with a finite buffer, and we run
# unbuffered (python -u). If the platform ever stops draining that pipe, the
# next write() blocks -- and since every thread logs, EVERY thread stops at
# once, with the container still reporting ACTIVE and no crash to restart.
# That is the exact signature of the 29-hour silent freeze on 2026-08-30.
# Records now go onto a bounded queue that DROPS when full, so a stalled log
# consumer costs us log lines instead of costing us the trading loops.
NEWLINE = "\n"

_log_q = queue.Queue(maxsize=5000)


class _DropWhenFull(logging.handlers.QueueHandler):
    """Never block a worker thread just to emit a log line."""

    def prepare(self, record):
        # Same process, so no pickling is needed. The default prepare()
        # pre-formats the record and the listener then formats it again,
        # which double-prefixes every line.
        return record

    def prepare(self, record):
        # Same process, so no pickling is needed. The default prepare()
        # pre-formats the record and the listener then formats it again,
        # which double-prefixes every line.
        return record

    def enqueue(self, record):
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            pass


_log_stream = logging.StreamHandler(sys.stdout)
_log_stream.setFormatter(
    logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_DropWhenFull(_log_q)])
_log_listener = logging.handlers.QueueListener(_log_q, _log_stream)
_log_listener.start()
log = logging.getLogger("alertbot")

# De-dup memory so the same event isn't posted twice.
_seen = set()

# symbol -> [epoch timestamps of catalyst alerts] for the rolling 24h throttle.
_news_hist = defaultdict(list)

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
    # The news throttle is a rolling 24h window, so restore it regardless of the
    # calendar date and just drop anything already outside the window.
    cutoff = time.time() - NEWS_WINDOW_SEC
    restored = 0
    for sym, stamps in (blob.get("news_hist") or {}).items():
        keep = [t for t in stamps if t >= cutoff]
        if keep:
            _news_hist[sym] = keep
            restored += 1
    if restored:
        log.info("Restored 24h news throttle for %d tickers", restored)


def _save_seen():
    """Atomically write the current de-dup set (with today's date) to disk."""
    try:
        tmp = SEEN_PATH + ".tmp"
        cutoff = time.time() - NEWS_WINDOW_SEC
        hist = {s: [t for t in ts if t >= cutoff] for s, ts in _news_hist.items()}
        hist = {s: ts for s, ts in hist.items() if ts}
        with open(tmp, "w") as f:
            json.dump({"date": time.strftime("%Y%m%d"), "seen": sorted(_seen),
                       "news_hist": hist}, f)
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

# Market cap for EVERY symbol on the screener (not just the small-cap universe),
# so the fleet-wide cap can be enforced on halts/news for names that fall outside
# the price band. Guarded by _universe_lock.
_mcap_all = {}

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
                # Cap the wait: Telegram can hand back a retry_after of many
                # minutes, and sleeping that long here stalls the whole scheduler
                # because this runs on the main thread holding _send_lock.
                nap = min(retry, MAX_TG_SLEEP)
                log.warning("Telegram 429; sleeping %ss (asked %ss) then retrying",
                            nap, retry)
                time.sleep(nap + 1)
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


_day_ctx_cache = {}     # sym -> (yyyymmdd, today's open, median prior volume)


def day_context(sym):
    """(today's open, median prior daily volume) from one cached historical call.

    Both come from the same request and neither changes during a session, so
    this is fetched once per symbol per day.
    """
    day = now_et().strftime("%Y%m%d")
    hit = _day_ctx_cache.get(sym)
    if hit and hit[0] == day:
        return hit[1], hit[2]
    n = now_et()
    url = NASDAQ_HIST.format(sym=sym,
                             frm=(n - timedelta(days=21)).strftime("%Y-%m-%d"),
                             to=n.strftime("%Y-%m-%d"))
    op = avg = None
    try:
        r = _http.get(url, headers=NASDAQ_HEADERS, timeout=12)
        if r.status_code == 200:
            tbl = (r.json().get("data") or {}).get("tradesTable") or {}
            rows = tbl.get("rows") or []
            if rows:
                op = _num(rows[0].get("open"))
            # rows[0] is today; the baseline is the sessions before it.
            vols = [v for v in (_num(x.get("volume")) for x in rows[1:11]) if v]
            if len(vols) >= 3:
                vols.sort()
                avg = vols[len(vols) // 2]
    except (requests.RequestException, ValueError, AttributeError):
        pass
    _day_ctx_cache[sym] = (day, op, avg)
    return op, avg


_ipo_cache = {}         # {'day': yyyymmdd, 'syms': set()}
_unlisted_seen = set()  # symbols seen in the halt feed but absent from the universe


def avg_daily_volume(sym):
    """Median prior daily volume. Shares day_context's cached fetch so the
    RVOL paths and the runner confirmation never request the same symbol twice.
    """
    return day_context(sym)[1]


def too_big(sym):
    """Fleet-wide gate: True if this company exceeds MAX_MARKET_CAP.

    Symbols with no market-cap data are NOT filtered -- those are almost always
    obscure micro-caps, and silently dropping them would defeat the purpose.
    """
    with _universe_lock:
        mc = _mcap_all.get(sym)
    return mc is not None and mc > MAX_MARKET_CAP


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
    caps = {}
    for row in rows:
        sym = (row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        mcap = _num(row.get("marketCap"))
        if mcap is not None:
            caps[sym] = mcap        # recorded for EVERY symbol, price-band or not
        price = _num(row.get("lastsale"))
        if price is None:
            continue
        if "^" in sym or "/" in sym or "." in sym:
            continue
        if price < PRICE_MIN or price > PRICE_MAX:
            continue
        if mcap is not None and mcap > MAX_MARKET_CAP:
            continue
        uni[sym] = {
            "close": price,
            "mcap": mcap,
            "name": (row.get("name") or "").strip(),
            "vol": _num(row.get("volume")) or 0.0,
            "pct": _num(str(row.get("pctchange") or "").replace("%", "")) or 0.0,
        }
    # Company-name -> ticker index, so a PR headline that says "Expion" or
    # "Reitar Logtech" (and never prints the symbol) can still be matched.
    nidx, npre = {}, {}
    dupes = set()
    for sym, info in uni.items():
        key = _name_key(info.get("name"))
        if not key:
            continue
        if key in nidx and nidx[key] != sym:
            dupes.add(key)                # ambiguous name -> unusable
            continue
        nidx[key] = sym
        npre.setdefault(key[:5], []).append((key, sym))
    for k in dupes:
        nidx.pop(k, None)

    with _universe_lock:
        _universe.clear()
        _universe.update(uni)
        _mcap_all.clear()
        _mcap_all.update(caps)
        _name_index.clear()
        _name_index.update(nidx)
        _name_prefix.clear()
        _name_prefix.update(npre)
    log.info("Universe: %d rows -> %d symbols under $%s cap (%d caps known)",
             len(rows), len(uni), format(MAX_MARKET_CAP, ","), len(caps))


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
    pool = []               # top-10 board candidates (wider price band)
    quiet_heavy = []        # big turnover, no big price move (yet)
    for row in rows:
        sym = (row.get("symbol") or "").strip().upper()
        price = _num(row.get("lastsale"))
        pct = _num(row.get("pctchange"))
        vol = _num(row.get("volume"))
        if not sym or price is None or pct is None or vol is None:
            continue
        if "^" in sym or "/" in sym or "." in sym:
            continue
        mcap = _num(row.get("marketCap"))
        # Board pool: a wider price band than the alert universe, taken from the
        # screener pass we are already making, so it costs no extra requests.
        if (BOARD_PRICE_MIN <= price <= BOARD_PRICE_MAX
                and (mcap is None or mcap <= MAX_MARKET_CAP)
                and (price * vol) >= BOARD_MIN_DOLLAR):
            pool.append({"symbol": sym, "price": price, "pct": pct,
                         "dollar": price * vol})
        if price < PRICE_MIN or price > PRICE_MAX:
            continue
        if mcap is not None and mcap > MAX_MARKET_CAP:
            continue
        if pct < MIN_PERCENT:
            # Not a mover on price -- but heavy turnover on a flat tape is the
            # accumulation setup that precedes the run, so keep it as a candidate.
            if (pct >= 0 and vol >= RVOL_MIN_SHARES
                    and (price * vol) >= RVOL_MIN_DOLLAR):
                quiet_heavy.append({"symbol": sym, "price": price,
                                    "pct": pct, "volume": vol})
            continue
        if vol < MIN_VOLUME or (price * vol) < MIN_DOLLAR_VOL:
            continue
        out.append({"symbol": sym, "price": price, "pct": pct, "volume": vol})

    out.sort(key=lambda d: d["pct"], reverse=True)

    pool.sort(key=lambda d: d["pct"], reverse=True)
    with _board_lock:
        _board_pool[:] = pool[:BOARD_POOL_MAX]
        _board_pool_ts[0] = time.time()

    # Resolve relative volume for the heaviest quiet names only (each baseline is
    # one extra request, cached per day, so this is capped per scan).
    quiet_heavy.sort(key=lambda d: d["volume"], reverse=True)
    unusual = []
    for c in quiet_heavy[:RVOL_LOOKUPS_MAX]:
        base = avg_daily_volume(c["symbol"])
        if not base:
            continue
        rvol = c["volume"] / base
        if rvol >= RVOL_MIN:
            c["rvol"] = rvol
            unusual.append(c)
    unusual.sort(key=lambda d: d["rvol"], reverse=True)

    log.info("Scanner: %d rows -> %d movers, %d unusual-volume", len(rows),
             len(out), len(unusual))
    return out, unusual


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
    movers, unusual = discover_movers()

    day = now_et().strftime("%Y%m%d")
    for u in unusual:
        sym = u["symbol"]
        if not once("unusual:" + sym + ":" + day):
            continue
        _catalyst.add(sym)          # worth watching in the next extended sweep
        send_telegram(
            "\U0001F50A <b>UNUSUAL VOLUME</b>\n"
            + "<b>" + html.escape(sym) + "</b>  $" + format(u["price"], ",.2f")
            + "  " + format(u["rvol"], ",.0f") + "x avg vol"
        )

    if not movers:
        return

    desired = {m["symbol"] for m in movers[:MAX_WATCH]}
    with _watch_lock:
        _watchlist.clear()
        _watchlist.update(desired)
    _ws_sync(desired)

    day = now_et().strftime("%Y%m%d")
    alerted = 0
    ctx_lookups = 0
    for m in movers:
        if alerted >= TOP_N_ALERTS:
            break
        sym = m["symbol"]
        if not once("runner:" + sym + ":" + day):
            continue
        # Confirm the move: it must be carrying unusual volume AND have travelled
        # intraday, not just gapped and gone flat. NAKA alerted on 0.75x average
        # volume and WRAP on 0.99x -- no surge at all -- and both stalled.
        if ctx_lookups < RUNNER_CTX_LOOKUPS:
            open_px, base = day_context(sym)
            ctx_lookups += 1
        else:
            open_px = base = None
        rv = (m["volume"] / base) if base else None
        if rv is not None and rv < RUNNER_RVOL_MIN:
            continue
        if open_px and open_px > 0:
            from_open = (m["price"] - open_px) / open_px * 100.0
            if from_open < RUNNER_MIN_FROM_OPEN:
                continue
        if rv is None:
            log.warning("No volume baseline for %s; alerting unconfirmed", sym)
        rv_txt = ("  (" + format(rv, ",.1f") + "x avg)") if rv else ""
        send_telegram(
            "\U0001F680 <b>SMALL-CAP RUNNER</b>\n"
            + "<b>" + html.escape(sym) + "</b>  $" + format(m["price"], ",.2f")
            + "  (" + format(m["pct"], "+.1f") + "%)\n"
            + "Vol: " + format(int(m["volume"]), ",") + rv_txt + lowfloat_tag(sym)
        )
        alerted += 1


# ----------------------------------------------------------------------
# ALERT 2: PREMARKET / AFTER-HOURS RUNNERS  (Nasdaq extended-trading, free)
# ----------------------------------------------------------------------
_PCT_RE = re.compile(r"\(([+-]?[\d.]+)\s*%\)")
_PRICE_RE = re.compile(r"\$([\d.]+)")


def parse_extended(payload, base=None):
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
        high = float(hm.group(1)) if hm else None
        # The consolidated last-trade badly lags a fast tape: TNON printed
        # "+1.00%" here while the live quote was +35.66% and the session high was
        # +63%. Derive the move off the session high vs the prior close too, and
        # trigger on whichever is larger, or fast movers slip straight past us.
        pv = _PRICE_RE.search(str(d.get("previousInfo") or ""))
        prev = float(pv.group(1)) if pv else None
        # After the close, previousInfo still reports YESTERDAY close. AEHL
        # closed -30% at $3.54 then ran to $6.23 after hours -- a +76% move
        # the feed labelled "+2.28%" because it still measured off $5.09.
        # When the caller knows today close, that is the only honest base.
        price = float(pm.group(1))
        pct = float(pc.group(1))
        if base and base > 0:
            prev = base
            pct = (price - base) / base * 100.0
        pct_high = (((high - prev) / prev) * 100.0
                    if (high and prev and prev > 0) else None)
        return {
            "price": price,
            "pct": pct,
            "volume": _num(row.get("volume")) or 0.0,
            "high": high,
            "prev": prev,
            "pct_high": pct_high,
        }
    except (AttributeError, TypeError, ValueError):
        return None


def fetch_extended(sym, markettype):
    """One symbol's extended-hours quote.

    Returns a dict on success, "ERR" on an HTTP/transport failure, or None when
    the symbol simply has no extended-hours trades (the common case -- most small
    caps never print premarket). Callers must count those two apart: lumping them
    together made a normal quiet market look like a 72% failure rate.
    """
    url = NASDAQ_EXTENDED.format(sym=sym, mt=markettype)
    base = None
    if markettype == "post":
        with _universe_lock:
            base = (_universe.get(sym) or {}).get("close")
    try:
        r = _http.get(url, headers=NASDAQ_HEADERS, timeout=EXT_TIMEOUT)
    except requests.RequestException:
        return "ERR"
    if r.status_code != 200:
        return "ERR"
    try:
        return parse_extended(r.json(), base)
    except ValueError:
        return "ERR"


# Top-10 board state.
_board_pool = []         # regular session: [{symbol, price, pct, dollar}, ...]
_board_pool_ts = [0.0]
_ext_board = {}          # extended sessions: sym -> (ts, price, pct, dollar)
_board_seen = {}         # "yyyymmdd:session" -> set of symbols already posted
_board_last = {"key": None, "ts": 0.0}
_board_lock = threading.Lock()


_ext_cursor = [0]        # rotates the long tail across sweeps


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
    # Today movers first, whichever way they moved. AEHL closed -30% on 6x
    # volume then doubled after hours; ranked by PRIOR-day turnover it sat
    # ~2,500 names deep in a sweep that never reached it.
    movers = sorted(
        (s for s, i in uni.items()
         if abs(i.get("pct") or 0.0) >= EXT_PRIORITY_PCT
         and (i.get("vol") or 0.0) >= EXT_PRIORITY_VOL),
        key=lambda s: abs(uni[s].get("pct") or 0.0), reverse=True)
    ordered, seen = [], set()
    for group in (sorted(_catalyst), sorted(_watchlist), movers):
        for s in group:
            if s in uni and s not in seen:
                ordered.append(s)
                seen.add(s)
    # The long tail is real but slow: 2,891 symbols at 5 workers took over an
    # hour per sweep, so anything past the first few hundred was never polled
    # while it mattered. Rotate through it a chunk at a time instead -- full
    # coverage every few sweeps, and every sweep actually finishes.
    tail = [s for s, _i in sorted(
        uni.items(),
        key=lambda kv: (kv[1].get("vol") or 0) * (kv[1].get("close") or 0),
        reverse=True) if s not in seen]
    if tail:
        start = _ext_cursor[0] % len(tail)
        chunk = tail[start:start + EXT_TAIL_CHUNK]
        if len(chunk) < EXT_TAIL_CHUNK:
            chunk += tail[:EXT_TAIL_CHUNK - len(chunk)]
        _ext_cursor[0] = (start + EXT_TAIL_CHUNK) % len(tail)
        ordered.extend(chunk)
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
    heavy = []
    fails = 0

    def probe(sym):
        return sym, fetch_extended(sym, markettype)

    errors = 0
    with ThreadPoolExecutor(max_workers=EXT_WORKERS) as pool:
        for sym, q in pool.map(probe, cands):
            if q == "ERR":
                errors += 1
                continue
            if not q:
                fails += 1          # no extended-hours trades: normal and expected
                continue
            price, pct, vol = q["price"], q["pct"], q["volume"]
            # Feed the top-10 board before any alert filter narrows things down:
            # the board has its own wider price band and its own thresholds.
            peak_pct = max(pct, q.get("pct_high") or pct)
            with _board_lock:
                _ext_board[sym] = (time.time(), price, peak_pct, price * vol)
            if price < PRICE_MIN or price > PRICE_MAX:
                continue
            if vol < PM_MIN_VOLUME:
                continue
            # Trigger on the bigger of "where it is now" and "where it got to".
            peak = max(pct, q.get("pct_high") or pct)
            if peak >= PM_MIN_PERCENT:
                hits.append((peak, sym, price, vol, q.get("high")))
            elif vol >= EXT_RVOL_MIN_SHARES:
                heavy.append((vol, sym, price, q.get("high")))

    # Heavy extended volume without a big price move yet. TNON traded 6.3x a
    # normal FULL day's volume before the open while its last print still read
    # +1% -- that is the signal, and it only exists in extended hours.
    heavy.sort(reverse=True)
    day = now_et().strftime("%Y%m%d")
    for vol, sym, price, high in heavy[:EXT_RVOL_LOOKUPS]:
        base = avg_daily_volume(sym)
        if not base:
            continue
        rv = vol / base
        if rv < EXT_RVOL_MIN:
            continue
        if not once("extvol:" + markettype + ":" + sym + ":" + day):
            continue
        _catalyst.add(sym)
        send_telegram(
            "\U0001F50A <b>" + label + " VOLUME</b>\n"
            + "<b>" + html.escape(sym) + "</b>  $" + format(price, ",.2f")
            + "  " + format(rv, ",.1f") + "x daily vol"
        )

    hits.sort(key=lambda h: h[0], reverse=True)   # biggest movers alert first
    sent = 0
    for pct, sym, price, vol, high in hits:
        if sent >= MAX_EXT_ALERTS:
            break
        # Re-alert only on a materially bigger move (every extra 50%).
        tier = int(pct // 50)
        if not once("ext:" + markettype + ":" + sym + ":" + day + ":" + str(tier)):
            continue
        # Minimal extended-hours format: ticker, price, chart emoji. No percent,
        # no session high, no volume, no low-float tag.
        send_telegram(
            "\U0001F680 <b>" + label + " RUNNER</b>\n"
            + "<b>" + html.escape(sym) + "</b>  $" + format(price, ",.2f")
            + " \U0001F4C8"
        )
        sent += 1
    log.info("%s sweep: %d symbols in %.0fs -> %d qualifying, %d alerts "
             "(%d no-premarket-trades, %d http errors)",
             label, len(cands), time.time() - t0, len(hits), sent, fails, errors)
    if errors > len(cands) * 0.2:
        log.warning("%s sweep: %d/%d requests errored -- Nasdaq may be throttling",
                    label, errors, len(cands))


def webull_movers(rank_type):
    """Market-wide extended-hours gainers, ranked, in one call.

    rank_type is "preMarket" or "afterMarket". Returns [] on any failure, which
    callers treat as "fall back to our own sweep" rather than as an error.
    """
    url = WEBULL_RANK_URL.format(rt=rank_type, n=WEBULL_PAGE)
    try:
        r = _http.get(url, headers=WEBULL_HEADERS, timeout=15)
        if r.status_code != 200:
            log.warning("Webull %s HTTP %s", rank_type, r.status_code)
            return []
        data = (r.json() or {}).get("data") or []
    except (requests.RequestException, ValueError) as e:
        log.warning("Webull %s failed: %s", rank_type, e)
        return []
    out = []
    for row in data:
        t = row.get("ticker") or {}
        v = row.get("values") or {}
        sym = (t.get("symbol") or "").strip().upper()
        price = _num(v.get("price"))
        ratio = _num(v.get("changeRatio"))
        if not sym or price is None or ratio is None:
            continue
        out.append({"symbol": sym, "price": price, "pct": ratio * 100.0,
                    "mcap": _num(t.get("marketValue")),
                    "tickerId": t.get("tickerId")})
    return out


def webull_quotes(ids):
    """Batch extended-hours quotes: price, percent AND volume in one call.

    The p-prefixed fields are the extended session -- premarket before the open,
    after-hours after the close -- which is exactly the move each board ranks.
    """
    ids = [str(i) for i in ids if i][:WEBULL_QUOTE_MAX]
    if not ids:
        return {}
    try:
        r = _http.get(WEBULL_QUOTE_URL.format(ids=",".join(ids)),
                      headers=WEBULL_HEADERS, timeout=15)
        if r.status_code != 200:
            log.warning("Webull quotes HTTP %s", r.status_code)
            return {}
        rows = r.json() or []
    except (requests.RequestException, ValueError) as e:
        log.warning("Webull quotes failed: %s", e)
        return {}
    out = {}
    for t in rows:
        sym = (t.get("symbol") or "").strip().upper()
        if not sym:
            continue
        out[sym] = {"price": _num(t.get("pPrice")),
                    "pct": _num(t.get("pChRatio")),
                    "volume": _num(t.get("pVolume")) or 0.0,
                    "mcap": _num(t.get("marketValue"))}
    return out


def _board_key():
    return now_et().strftime("%Y%m%d") + ":" + session()


def board_rows():
    """Ranked board candidates for whichever session is running now.

    Regular hours come from the market-wide screener, so the ranking is exact.
    Extended hours have no free market-wide feed -- Nasdaq's screener still
    reports the last REGULAR print before the open and after the close -- so
    those boards are built from what our own extended sweep has seen this
    session. Movers are re-polled every sweep and the rest of the market
    rotates through, so an extended board is best-effort, not exhaustive.
    """
    def eligible(r):
        return (BOARD_PRICE_MIN <= r["price"] <= BOARD_PRICE_MAX
                and r["dollar"] >= BOARD_MIN_DOLLAR
                and r["pct"] is not None
                and not too_big(r["symbol"]))

    sess = session()
    if sess == "regular":
        with _board_lock:
            if time.time() - _board_pool_ts[0] > 300:
                return []           # stale screener; skip rather than mislead
            return [r for r in _board_pool if eligible(r)]
    if sess not in ("pre", "post"):
        return []
    wb = webull_movers("preMarket" if sess == "pre" else "afterMarket")
    if wb:
        quotes = webull_quotes([r.get("tickerId") for r in wb])
        picked = []
        for r in wb:                    # already ranked by percent, descending
            q = quotes.get(r["symbol"]) or {}
            price = q.get("price") or r["price"]
            pct = q["pct"] * 100.0 if q.get("pct") is not None else r["pct"]
            mcap = q.get("mcap") if q.get("mcap") is not None else r.get("mcap")
            if not (BOARD_PRICE_MIN <= price <= BOARD_PRICE_MAX):
                continue
            if mcap is not None and mcap > MAX_MARKET_CAP:
                continue
            if too_big(r["symbol"]):
                continue
            dollar = price * q.get("volume", 0.0)
            if dollar < BOARD_MIN_DOLLAR:
                continue
            picked.append({"symbol": r["symbol"], "price": price,
                           "pct": pct, "dollar": dollar})
            if len(picked) >= BOARD_N:
                break
        if picked:
            # Anything ranking market-wide deserves a front-of-queue probe on
            # the next sweep, so the runner alerts see it too.
            for r in picked:
                _catalyst.add(r["symbol"])
            return picked
        log.info("Board (%s): Webull gave %d rows, none cleared the filters; "
                 "falling back to the sweep", sess, len(wb))

    cutoff = time.time() - 1800     # drop anything not seen in 30 minutes
    rows = []
    with _board_lock:
        items = list(_ext_board.items())
    for sym, (ts, price, pct, dollar) in items:
        if ts < cutoff:
            continue
        r = {"symbol": sym, "price": price, "pct": pct, "dollar": dollar}
        if eligible(r):
            rows.append(r)
    rows.sort(key=lambda d: d["pct"], reverse=True)
    return rows


def _board_line(i, r):
    return (format(i, "2d") + ". <b>" + html.escape(r["symbol"]) + "</b>  $"
            + format(r["price"], ",.2f") + "  (" + format(r["pct"], "+.1f") + "%)")


def check_top_board():
    """Post the session's top gainers, then only new entrants after that.

    The rule is do not re-post a name that is already on the board: the full
    list goes out once when a session's board first forms, and from then on the
    only messages are names that have newly broken into the top BOARD_N.
    """
    sess = session()
    if sess not in ("pre", "regular", "post"):
        return
    # Re-rank every INTERVAL_BOARD, but never make a new session wait for the
    # timer -- the opening board should land when the session opens.
    key0 = _board_key()
    if (key0 == _board_last["key"]
            and time.time() - _board_last["ts"] < INTERVAL_BOARD):
        return
    rows = board_rows()
    if len(rows) < BOARD_MIN_OPEN:
        return
    top = rows[:BOARD_N]
    key = _board_key()
    label = {"pre": "PREMARKET", "regular": "REGULAR HOURS",
             "post": "AFTER-HOURS"}[sess]
    with _board_lock:
        already = _board_seen.get(key)
        first = already is None
        if first:
            already = set()
            _board_seen[key] = already
        fresh = [r for r in top if r["symbol"] not in already]
        for r in top:
            already.add(r["symbol"])
    _board_last["key"] = key
    _board_last["ts"] = time.time()
    if first:
        body = NEWLINE.join(_board_line(i, r) for i, r in enumerate(top, 1))
        send_telegram("\U0001F3C6 <b>TOP GAINERS - " + label + "</b>\n" + body)
        log.info("Board (%s): opened with %d names", sess, len(top))
        return
    if not fresh:
        return
    body = NEWLINE.join(
        "<b>" + html.escape(r["symbol"]) + "</b>  $" + format(r["price"], ",.2f")
        + "  (" + format(r["pct"], "+.1f") + "%)" for r in fresh)
    send_telegram("\U0001F3C6 <b>NEW ON THE " + label + " TOP "
                  + str(BOARD_N) + "</b>\n" + body)
    log.info("Board (%s): %d new entrant(s)", sess, len(fresh))


def _watchdog():
    """Kill the process if the main scheduler loop stops ticking.

    Exiting non-zero triggers Railway's restart-on-failure policy, so a wedge
    costs ~10 minutes of downtime instead of silently lasting all day.

    This must NEVER touch the logger. The previous version logged its warning
    first and blocked on that very write for 29 hours on 2026-08-30 -- the one
    thread whose job was to rescue us was the one thing the wedge could stop.
    """
    while True:
        time.sleep(30)
        if time.time() - _last_tick[0] <= WATCHDOG_TIMEOUT:
            continue
        # Guarantee the exit even if the raw write below blocks too.
        threading.Thread(target=lambda: (time.sleep(5), os._exit(1)),
                         daemon=True).start()
        try:
            os.write(2, b"WATCHDOG: main loop stalled -- exiting for restart\n")
        except OSError:
            pass
        os._exit(1)


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

# --- English-only gate for catalyst/news headlines -------------------------
# Any non-Latin script is an immediate reject.
_NON_LATIN_RE = re.compile(
    "["
    "\u0400-\u04ff"    # Cyrillic
    "\u0590-\u05ff"    # Hebrew
    "\u0600-\u06ff"    # Arabic
    "\u0370-\u03ff"    # Greek
    "\u0e00-\u0e7f"    # Thai
    "\u0900-\u097f"    # Devanagari
    "\u3040-\u30ff"    # Japanese kana
    "\u4e00-\u9fff"    # CJK
    "\uac00-\ud7af"    # Hangul
    "]"
)
_ACCENT_RE = re.compile("[\u00e0-\u00ff]")
_WORD_RE = re.compile("[A-Za-z\u00c0-\u00ff']+")

_FOREIGN_WORDS = {
    # Spanish / Portuguese
    "anuncia", "anuncio", "acuerdo", "para", "con", "del", "los", "las", "una",
    "por", "sus", "empresa", "mercado", "millones", "segun", "tambien", "mas",
    "nao", "com", "apos", "sobre", "sera", "esta", "sao", "dos", "das", "pela",
    "pelo", "um", "uma", "su", "sistema", "de", "el", "en", "que", "ano",
    # French
    "societe", "avec", "pour", "dans", "sur", "les", "des", "une", "aux", "est",
    "son", "resultats", "annonce", "ete", "leur", "cette", "ses",
    # German
    "und", "der", "die", "das", "fuer", "mit", "von", "bei", "auf", "eine",
    "einen", "wird", "aktie", "millionen", "geschaeftsjahr", "quartal", "nicht",
    # Italian
    "della", "dello", "degli", "nel", "gli", "sono", "anche", "societa",
}
_ENGLISH_WORDS = {
    "the", "and", "of", "to", "in", "for", "on", "with", "announces", "announced",
    "reports", "is", "at", "its", "from", "new", "first", "second", "third",
    "fourth", "quarter", "results", "company", "shares", "will", "has", "have",
    "by", "as", "that", "after", "said", "says", "million", "billion", "stock",
    "agreement", "completes", "receives", "launches", "expands", "signs", "into",
    "up", "over", "market", "board", "chief", "officer", "inc", "corp",
}


def is_english(text):
    """Heuristic: keep only English-looking headlines (no translation, just a gate).

    Deliberately conservative -- an English headline containing one accented
    proper noun still passes; it takes a majority of foreign function words, or a
    non-Latin script, to reject.
    """
    if not text:
        return True
    if _NON_LATIN_RE.search(text):
        return False
    words = [w.lower() for w in _WORD_RE.findall(text)]
    if not words:
        return True
    foreign = sum(1 for w in words if w in _FOREIGN_WORDS)
    english = sum(1 for w in words if w in _ENGLISH_WORDS)
    if foreign > english:
        return False
    # Heavy accent use with zero English function words -> not English.
    if english == 0 and len(_ACCENT_RE.findall(text.lower())) >= 2:
        return False
    return True


# PR wires tag each release with the listing, e.g. <category>Nasdaq:ENGS</category>.
# The headline itself almost never contains the symbol -- it leads with the company
# name -- so the category tag is the reliable place to find the ticker.
_EXCH_TICKER_RE = re.compile(
    r"(?:NASDAQ|NYSE\s*AMERICAN|NYSEAMERICAN|NYSE|AMEX|CBOE|OTCQB|OTCQX|OTC)"
    r"\s*:\s*([A-Z][A-Z0-9]{0,5})", re.I)

# Generic words that must never be treated as a company's distinctive name.
_NAME_STOP = {
    "the", "and", "for", "inc", "corp", "ltd", "plc", "llc", "co", "group",
    "holdings", "holding", "company", "american", "global", "national", "first",
    "international", "technologies", "technology", "industries", "systems",
    "solutions", "capital", "financial", "enterprises", "pharmaceuticals",
    "pharma", "therapeutics", "energy", "resources", "partners", "trust", "fund",
    "acquisition", "limited", "common", "stock", "shares", "ordinary", "class",
    "depositary", "sciences", "biosciences", "medical", "health", "healthcare",
    "united", "general", "standard", "premier", "advanced", "digital", "data",
}

_name_index = {}    # distinctive first token -> symbol (unique matches only)
_name_prefix = {}   # first 5 chars -> [(token, symbol)] for prefix matching


def _name_key(name):
    """First distinctive token of a company name, or None."""
    for tok in re.findall(r"[a-z0-9]+", (name or "").lower()):
        if len(tok) >= 5 and tok not in _NAME_STOP:
            return tok
    return None


def companies_in(text):
    """Symbols whose COMPANY NAME appears in the text.

    Press releases say "Expion Acquires..." or "Reitar Logtech forms...", never
    "XPON" or "RITR". Matching on names is what turns those into alerts. Prefix
    matching both ways handles "Expion" vs the listed name "Expion360".
    """
    if not text:
        return set()
    with _universe_lock:
        idx = _name_index
        pre = _name_prefix
        if not idx:
            return set()
        out = set()
        for tok in {t for t in re.findall(r"[a-z0-9]+", text.lower()) if len(t) >= 5}:
            sym = idx.get(tok)
            if sym:
                out.add(sym)
                continue
            cands = [s for k, s in pre.get(tok[:5], [])
                     if k.startswith(tok) or tok.startswith(k)]
            if len(set(cands)) == 1:      # ambiguous prefixes are dropped
                out.add(cands[0])
        return out


def tickers_from_entry(entry):
    """Tickers for an RSS item: category tags first, then any Exchange:SYM in text."""
    found = set()
    for tag in (entry.get("tags") or []):
        term = tag.get("term", "") if isinstance(tag, dict) else str(tag)
        for m in _EXCH_TICKER_RE.finditer(term or ""):
            found.add(m.group(1).upper())
    blob = (entry.get("title", "") or "") + " " + (entry.get("summary", "") or "")
    for m in _EXCH_TICKER_RE.finditer(blob):
        found.add(m.group(1).upper())
    with _universe_lock:
        return {t for t in found if t in _universe}


def tickers_in(text):
    """Uppercase tokens in text that are real symbols in our small-cap universe."""
    if not text:
        return set()
    with _universe_lock:
        uni = _universe
        return {t for t in _TICKER_RE.findall(text.upper()) if t in uni}


def _news_alert(sym, headline, source, url, tag="NEWS"):
    if too_big(sym):        # fleet-wide market-cap gate
        return False
    if not is_english(headline):    # English-only channel
        return False
    # Rolling 24h throttle: the same story reaches us from several wires under
    # different ids, so cap catalyst alerts per ticker regardless of source.
    now_ts = time.time()
    hist = [t for t in _news_hist.get(sym, []) if now_ts - t < NEWS_WINDOW_SEC]
    if len(hist) >= NEWS_MAX_PER_24H:
        _news_hist[sym] = hist
        return False
    with _universe_lock:
        info = _universe.get(sym) or {}
    px = info.get("close")
    price_str = ("  $" + format(px, ",.2f")) if px else ""
    send_telegram(
        "\U0001F4F0 <b>" + tag + "</b>  <b>" + html.escape(sym) + "</b>" + price_str + "\n"
        + html.escape(headline[:250])
    )
    hist.append(now_ts)
    _news_hist[sym] = hist
    return True


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
            syms = (tickers_in(related) or tickers_in(headline)
                    or companies_in(headline))
            if not syms:
                continue
            for sym in list(syms)[:2]:
                _catalyst.add(sym)
                if once(nid + ":" + sym):
                    if _news_alert(sym, headline, str(n.get("source") or ""), url):
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
                # category tag (Nasdaq:XPON) -> Exchange:SYM in text -> company name
                syms = (tickers_from_entry(entry)
                        or tickers_in(title)
                        or companies_in(title))
                if not syms:
                    continue
                for sym in list(syms)[:2]:
                    _catalyst.add(sym)
                    key = "pr:" + (entry.get("id") or link or title)[:120] + ":" + sym
                    if once(key):
                        if _news_alert(sym, title, "PR Wire", link, tag="CATALYST"):
                            found += 1

    if found:
        log.info("News scan: %d new catalyst alerts (%d tickers tracked)",
                 found, len(_catalyst))


# ----------------------------------------------------------------------
# ALERT 7: SEC EDGAR FILINGS  (queued, alerted only when the tape confirms)
# ----------------------------------------------------------------------
# AEHL's $19M private placement never crossed a PR wire -- it surfaced in an SEC
# filing that Reuters and TipRanks picked up, so every wire-based feed we run was
# blind to it. EDGAR closes that gap. But EDGAR is a firehose: filings alone
# would bury the channel, so nothing here alerts on a filing by itself.
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_RECENT_URL = ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent"
                  "&type={typ}&dateb=&owner=include&count=100&output=atom")
# The forms that actually move small caps: material events, foreign-issuer
# reports (AEHL files 6-K), priced offerings/shelf takedowns, and new 5% stakes.
SEC_FORMS = ("8-K", "6-K", "424B5", "SC 13D")
_FORM_LABEL = {
    "8-K": "8-K material event",
    "6-K": "6-K foreign issuer report",
    "424B5": "424B5 offering priced (shelf takedown)",
    "SC 13D": "SC 13D new 5%+ stake",
}
# The SEC asks automated clients to identify themselves. Set SEC_CONTACT to your
# own email in Railway; the default is deliberately generic.
SEC_CONTACT = os.getenv("SEC_CONTACT") or "stock-alert-bot (automated market monitor)"
SEC_HEADERS = {"User-Agent": SEC_CONTACT, "Accept-Encoding": "gzip, deflate"}

_CIK_RE = re.compile(r"\((\d{7,10})\)")
_cik_map = {}            # {"day": yyyymmdd, "map": {cik_int: ticker}}
_edgar_pending = {}      # sym -> (first_seen_ts, form, company_name)
_edgar_lock = threading.Lock()


def sec_ticker_map():
    """CIK -> ticker for every SEC registrant. One file, refreshed once a day."""
    day = now_et().strftime("%Y%m%d")
    if _cik_map.get("day") == day and _cik_map.get("map"):
        return _cik_map["map"]
    try:
        r = _http.get(SEC_TICKERS_URL, headers=SEC_HEADERS, timeout=20)
        if r.status_code != 200:
            log.warning("SEC ticker map HTTP %s", r.status_code)
            return _cik_map.get("map") or {}
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        log.warning("SEC ticker map failed: %s", e)
        return _cik_map.get("map") or {}
    rows = data.values() if isinstance(data, dict) else data
    m = {}
    for row in rows:
        try:
            m[int(row["cik_str"])] = str(row["ticker"]).upper()
        except (KeyError, TypeError, ValueError):
            continue
    if m:
        _cik_map.clear()
        _cik_map.update({"day": day, "map": m})
        log.info("SEC ticker map: %d CIKs", len(m))
    return m


def _sec_headline(form, name):
    label = _FORM_LABEL.get(form, form + " filing")
    return "SEC " + label + " - " + name


def poll_edgar():
    """Queue new filings for tickers in our small-cap universe. Alerts nothing."""
    if feedparser is None:
        return
    cmap = sec_ticker_map()
    if not cmap:
        return
    queued = 0
    for form in SEC_FORMS:
        try:
            r = _http.get(SEC_RECENT_URL.format(typ=form.replace(" ", "+")),
                          headers=SEC_HEADERS, timeout=EDGAR_TIMEOUT)
            if r.status_code != 200:
                log.warning("EDGAR %s HTTP %s", form, r.status_code)
                continue
            feed = feedparser.parse(r.content)
        except requests.RequestException as e:
            log.warning("EDGAR %s fetch failed: %s", form, e)
            continue
        for entry in getattr(feed, "entries", [])[:60]:
            title = entry.get("title", "")
            cm = _CIK_RE.search(title)
            if not cm:
                continue
            sym = cmap.get(int(cm.group(1)))
            if not sym:
                continue
            with _universe_lock:
                if sym not in _universe:
                    continue        # outside the small-cap price/cap band
            if not once("edgar:" + sym + ":" + (entry.get("id") or title)[:120]):
                continue
            name = title.split(" - ", 1)[1] if " - " in title else title
            name = _CIK_RE.sub("", name).replace("(Filer)", "").strip(" -")
            with _edgar_lock:
                if len(_edgar_pending) < EDGAR_MAX_PENDING:
                    _edgar_pending[sym] = (time.time(), form, name)
            queued += 1
    if queued:
        log.info("EDGAR: %d filings queued (%d awaiting confirmation)",
                 queued, len(_edgar_pending))


def _edgar_loop():
    """Poll EDGAR on its own thread.

    Four form types against a slow SEC endpoint can take a minute or more.
    The main scheduler runs the halt feed every 20s and must not queue behind
    it."""
    while True:
        started = time.time()
        try:
            poll_edgar()
        except Exception as e:  # noqa: BLE001
            log.error("poll_edgar failed: %s", e)
        time.sleep(max(30, INTERVAL_EDGAR - (time.time() - started)))


def confirm_edgar():
    """Alert a queued filing only once the stock is actually doing something.

    Confirmation is any ONE of: a real move up, unusual turnover, or the ticker
    already being flagged by another alert today. No move, no alert -- the
    filing simply expires. Downside moves never confirm.
    """
    with _edgar_lock:
        pending = dict(_edgar_pending)
    if not pending:
        return
    now_ts = time.time()
    done = [s for s, (ts, _f, _n) in pending.items()
            if now_ts - ts > EDGAR_PENDING_SEC]
    lookups = 0
    sent = 0
    for sym, (ts, form, name) in sorted(pending.items()):
        if sym in done:
            continue
        with _universe_lock:
            info = dict(_universe.get(sym) or {})
        pct = info.get("pct") or 0.0
        vol = info.get("vol") or 0.0
        if pct < 0:
            continue                    # never alert a filing into a decline
        ok = pct >= EDGAR_MIN_PCT
        if not ok and (sym in _catalyst or sym in _watchlist):
            ok = True                   # already moving on another signal today
        if not ok and vol >= RVOL_MIN_SHARES and lookups < EDGAR_RVOL_LOOKUPS:
            base = avg_daily_volume(sym)
            lookups += 1
            ok = bool(base) and (vol / base) >= EDGAR_MIN_RVOL
        if not ok:
            continue
        if _news_alert(sym, _sec_headline(form, name), "SEC EDGAR", "",
                       tag="SEC FILING"):
            sent += 1
        _catalyst.add(sym)
        done.append(sym)
    if done:
        with _edgar_lock:
            for s in done:
                _edgar_pending.pop(s, None)
    if sent:
        log.info("EDGAR: %d filings confirmed by the tape (%d still pending)",
                 sent, len(_edgar_pending))


def parse_extended_raw(payload):
    """Extended-hours figures WITHOUT requiring a percent change.

    A first-day listing has no prior close, so Nasdaq returns previousInfo=None
    and a bare consolidated like "$8.45" with no "(+x%)". parse_extended() needs
    that percent and returns None, which is why new listings were invisible.
    """
    try:
        d = (payload or {}).get("data") or {}
        rows = ((d.get("infoTable") or {}).get("rows") or [])
        if not rows:
            return None
        row = rows[0]
        pm = _PRICE_RE.search(str(row.get("consolidated") or ""))
        if not pm:
            return None
        hm = _PRICE_RE.search(str(row.get("highPrice") or ""))
        lm = _PRICE_RE.search(str(row.get("lowPrice") or ""))
        return {
            "price": float(pm.group(1)),
            "high": float(hm.group(1)) if hm else None,
            "low": float(lm.group(1)) if lm else None,
            "volume": _num(row.get("volume")) or 0.0,
            "has_prev": bool(str(d.get("previousInfo") or "").strip()),
        }
    except (AttributeError, TypeError, ValueError):
        return None


def ipo_calendar_symbols():
    """Recently priced IPO tickers for the current month (cached per day)."""
    day = now_et().strftime("%Y%m%d")
    if _ipo_cache.get("day") == day:
        return _ipo_cache.get("syms", set())
    syms = set()
    try:
        r = _http.get(NASDAQ_IPO.format(ym=now_et().strftime("%Y-%m")),
                      headers=NASDAQ_HEADERS, timeout=15)
        if r.status_code == 200:
            rows = ((r.json().get("data") or {}).get("priced") or {}).get("rows") or []
            for x in rows:
                s = (x.get("proposedTickerSymbol") or "").strip().upper()
                if s and re.fullmatch(r"[A-Z]{1,6}", s):
                    syms.add(s)
    except (requests.RequestException, ValueError, AttributeError):
        pass
    _ipo_cache.clear()
    _ipo_cache.update({"day": day, "syms": syms})
    return syms


def check_new_listings():
    """Alert on first-day listings (IPOs, direct listings, de-SPACs).

    These have NO prior close, so every percent-change filter in the system is
    blind to them -- yet they are often the wildest premarket names (PSQL ran
    $16.60 to $33.99 on debut). Candidates come from the IPO calendar and from
    any symbol in the halt feed we have never seen in the universe (new issues
    almost always print an IPO1/IPOQ halt at open). The move is measured across
    the session low-to-last range, since there is nothing else to anchor to.
    """
    sess = session()
    if sess not in ("pre", "regular", "post"):
        return
    with _universe_lock:
        known = set(_universe) | set(_mcap_all)
    cands = (ipo_calendar_symbols() | _unlisted_seen) - known
    if not cands:
        return
    markettype = "post" if sess == "post" else "pre"
    day = now_et().strftime("%Y%m%d")
    checked = 0
    for sym in sorted(cands)[:NEW_LISTING_MAX_POLL]:
        url = NASDAQ_EXTENDED.format(sym=sym, mt=markettype)
        try:
            r = _http.get(url, headers=NASDAQ_HEADERS, timeout=12)
            q = parse_extended_raw(r.json()) if r.status_code == 200 else None
        except (requests.RequestException, ValueError):
            q = None
        checked += 1
        time.sleep(0.2)
        if not q or q["has_prev"]:
            continue            # has a prior close -> not a first-day listing
        low, price, vol = q.get("low"), q["price"], q["volume"]
        if not low or low <= 0 or vol < NEW_LISTING_MIN_VOL:
            continue
        rng = (price - low) / low * 100.0
        peak = ((q["high"] - low) / low * 100.0) if q.get("high") else rng
        if peak < NEW_LISTING_MIN_RANGE:
            continue
        tier = int(peak // 50)
        if not once("newlist:" + sym + ":" + day + ":" + str(tier)):
            continue
        _catalyst.add(sym)
        hi = ("  \u2022  High $" + format(q["high"], ",.2f")) if q.get("high") else ""
        send_telegram(
            "\U0001F195 <b>NEW LISTING</b>\n"
            + "<b>" + html.escape(sym) + "</b>  $" + format(price, ",.2f")
            + "  (" + format(rng, "+.0f") + "% off low)" + hi
        )
    if checked:
        log.info("New-listing scan: polled %d candidates", checked)


def check_mover_news():
    """Pull catalysts for stocks the scanner has ALREADY flagged as moving.

    The public wires cap at ~20 items and drop most small-cap releases, so a
    catalyst like "Expion Acquires..." or "Reitar Logtech forms JV" can never
    reach us that way. Every runner we detect gets its company news fetched
    directly from Finnhub instead -- if a stock is up 30%+ there is almost
    always a release behind it, and this surfaces it.
    """
    with _watch_lock:
        syms = sorted(_watchlist)[:25]
    if not syms:
        return
    today = now_et().strftime("%Y-%m-%d")
    found = 0
    for sym in syms:
        items = finnhub_get("company-news",
                            {"symbol": sym, "from": today, "to": today})
        if not isinstance(items, list):
            continue
        for n in items[:3]:
            url = n.get("url") or ""
            headline = str(n.get("headline") or "")
            nid = "mnews:" + sym + ":" + str(n.get("id") or url)
            if not headline or not once(nid):
                continue
            if _news_alert(sym, headline, str(n.get("source") or ""), url,
                           tag="CATALYST"):
                found += 1
        time.sleep(0.3)             # stay under Finnhub 60 calls/min
    if found:
        log.info("Mover-news: %d catalyst alerts across %d movers", found, len(syms))


# ----------------------------------------------------------------------
# ALERT 4: TRADING HALTS
# ----------------------------------------------------------------------
def _resume_epoch(rdate, rtime):
    """'08/17/2026' + '14:36:10.093' (ET) -> epoch seconds, or None."""
    try:
        dt = datetime.strptime(rdate.strip() + " " + rtime.strip().split(".")[0],
                               "%m/%d/%Y %H:%M:%S")
    except (ValueError, TypeError, AttributeError):
        return None
    if ET is not None:
        dt = dt.replace(tzinfo=ET)
    try:
        return dt.timestamp()
    except (ValueError, OverflowError):
        return None


def check_halts():
    if feedparser is None:
        return
    try:
        feed = feedparser.parse(NASDAQ_HALT_RSS)
    except Exception as e:  # noqa: BLE001
        log.error("Halt feed error: %s", e)
        return
    for entry in feed.entries:
        # The feed's <title> is just the ticker (e.g. "NIPG"). Its <description>
        # is a raw HTML table -- deliberately ignored so it never hits the channel.
        sym = (entry.get("ndaq_issuesymbol")
               or entry.get("title", "")).strip().upper()
        if not sym or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,7}", sym):
            continue
        # Nasdaq's halt items carry NO <guid> and NO <link>, so feedparser falls
        # back to <title> -- which is just the ticker. Keying off that collapses
        # every halt of a symbol into one entry, so only the first of the day ever
        # alerted (WETO halted 7x in one session and we sent one). Key on the halt
        # timestamp instead so each pause is its own event.
        stamp = ((entry.get("ndaq_haltdate") or "") + " "
                 + (entry.get("ndaq_halttime") or "")).strip()
        # RESUMPTION: the same feed item later gains a ResumptionTradeTime. This
        # runs on every poll (before the halt de-dup below, which would `continue`
        # past it) so a halt we already announced can still report its reopen.
        rdate = (entry.get("ndaq_resumptiondate") or "").strip()
        rtime = (entry.get("ndaq_resumptiontradetime") or "").strip()
        if rdate and rtime:
            rts = _resume_epoch(rdate, rtime)
            if rts is not None and time.time() >= rts:
                if once("resume:" + sym + ":" + stamp) and not _silent:
                    if not too_big(sym):
                        rpx = current_price(sym)
                        rstr = ("  $" + format(rpx, ",.2f")) if rpx else ""
                        send_telegram(
                            "\u25b6\ufe0f <b>RESUMED</b>\n"
                            + "<b>" + html.escape(sym) + "</b>" + rstr
                        )

        eid = "halt:" + sym + ":" + (stamp or entry.get("id")
                                     or entry.get("link") or "")
        if not once(eid):
            continue
        if _silent:
            continue        # priming a redeploy: record the id, don't fetch or send
        with _universe_lock:
            unknown = sym not in _universe and sym not in _mcap_all
        if unknown:
            _unlisted_seen.add(sym)   # likely a brand-new listing
        if too_big(sym):              # fleet-wide market-cap gate
            continue
        _catalyst.add(sym)            # halted names are prime premarket candidates
        px = current_price(sym)
        price_str = ("  $" + format(px, ",.2f")) if px else ""
        code = (entry.get("ndaq_reasoncode") or "").strip().upper()
        reason = ""
        if code:
            desc = HALT_REASONS.get(code)
            reason = "\n" + html.escape(code + (" - " + desc if desc else ""))
        send_telegram(
            "\U0001F6A8 <b>TRADING HALT</b>\n"
            + "<b>" + html.escape(sym) + "</b>" + price_str + reason
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
    # EDGAR polling is slow and bursty; give it its own thread too.
    threading.Thread(target=_edgar_loop, daemon=True).start()

    schedule = [
        (refresh_universe,   INTERVAL_UNIVERSE),
        (scan_market,        INTERVAL_SCAN),
        (check_market_news,  INTERVAL_NEWS),
        (check_mover_news,   INTERVAL_MOVER_NEWS),
        (check_new_listings, INTERVAL_NEWLIST),
        (check_halts,        INTERVAL_HALTS),
        (check_volume_surge, INTERVAL_VOLROLL),
        (confirm_edgar,      INTERVAL_EDGAR_OK),
        (check_top_board,    60),
    ]
    # Kick off the scanners almost immediately so alerts start flowing.
    soon = (scan_market, check_market_news)
    next_run = {fn.__name__: time.time() + (3 if fn in soon else interval)
                for fn, interval in schedule}
    last_saved_n = len(_seen)
    last_save_ts = 0.0
    cur_day = now_et().strftime("%Y%m%d")
    last_beat = time.time()

    threading.Thread(target=_watchdog, daemon=True).start()

    while True:
        _last_tick[0] = time.time()     # watchdog liveness
        # New trading day: drop yesterday's de-dup keys. Without this the set
        # grows for the life of the process (it was only ever reset on restart).
        today = now_et().strftime("%Y%m%d")
        if today != cur_day:
            _seen.clear()
            with _board_lock:
                _board_seen.clear()
                _ext_board.clear()
            cur_day = today
            last_saved_n = 0
            log.info("New trading day %s: cleared de-dup memory", today)
        # Heartbeat -- if these stop, the loop is wedged.
        if time.time() - last_beat >= HEARTBEAT_SEC:
            log.info("heartbeat: session=%s seen=%d watchlist=%d catalysts=%d",
                     session(), len(_seen), len(_watchlist), len(_catalyst))
            last_beat = time.time()

        now = time.time()
        for fn, interval in schedule:
            if now >= next_run[fn.__name__]:
                try:
                    fn()
                except Exception as e:  # noqa: BLE001 - never let one feed kill the loop
                    log.error("%s failed: %s", fn.__name__, e)
                next_run[fn.__name__] = time.time() + interval
        # Persist at most once a minute. This used to fire on every change to
        # _seen -- i.e. constantly during a scan -- re-serialising a set of
        # thousands of keys and writing it to the network-backed volume each
        # time, on this thread. That was a large, needless CPU + I/O burn and a
        # place the loop could block.
        if len(_seen) != last_saved_n and time.time() - last_save_ts >= SAVE_MIN_GAP:
            _save_seen()
            last_saved_n = len(_seen)
            last_save_ts = time.time()
        time.sleep(1)


if __name__ == "__main__":
    main()
