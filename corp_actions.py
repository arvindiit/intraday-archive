#!/usr/bin/env python3
"""Splits and bonuses: the reason a price series lies.

    python3 corp_actions.py            fetch and store
    python3 corp_actions.py --show TCS what we hold for one stock
    python3 corp_actions.py --dry      fetch and print, write nothing

A stock that splits 1:5 goes from 3,400 to 680 overnight. Nothing
happened - you hold five pieces instead of one - but every series that
spans the date shows an eighty percent crash, and anything reading it
believes the crash.

The archive cannot protect itself from this, because bars are stored
exactly as traded and that is the right thing to store. What it needs is
the SEPARATE record of what changed, so a reader spanning the date can
put both sides on one basis. That record exists only on the day it is
published; six months later a 5x gap in a price series is indistinguishable
from a stock that really moved 5x, and no amount of re-fetching will say
which it was. That is why this runs daily and why it is worth running
before the archive is even full.

The output is a factor, and its meaning is fixed: MULTIPLY PRICES FROM
BEFORE THE EX-DATE BY IT to bring them onto the basis prices after the
ex-date use.

    split  Rs 10 -> Rs 2       factor 0.2     one share became five
    bonus  1:1                 factor 0.5     one free for each held
    bonus  3:5                 factor 0.625   three free for every five

Dividends are recorded and NOT adjusted for. The price drop on an
ex-dividend date is real money leaving the company, is small next to a
split, and the convention for intraday charts is to leave it. Recording it
means the decision can change later without another year of waiting.

Anything whose wording cannot be parsed with confidence is stored with a
NULL factor and counted in the summary. A wrong adjustment is worse than
none: none leaves a visible cliff somebody investigates, while a wrong one
produces a smooth series that is quietly incorrect.

Standard library only.
"""

import argparse
import http.cookiejar
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(HERE, "data"))
STORE = os.path.join(DATA_DIR, "silver", "corp_actions.json")

UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 "
                    "Safari/537.36",
      "Accept": "application/json, text/plain, */*",
      "Accept-Language": "en-GB,en;q=0.9"}
API = "https://www.nseindia.com/api/corporates-corporateActions?index=equities"
TIMEOUT = 30


def _opener():
    """A cookie-carrying opener, warmed on the NSE home page. The api host
    refuses requests that arrive without the cookies the site hands out.

    Same handshake newsfeed.py uses - NSE has one way in and it is worth
    doing it identically rather than discovering its moods twice.
    """
    jar = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    op.addheaders = list(UA.items())
    try:
        op.open(urllib.request.Request("https://www.nseindia.com/",
                                       headers=UA), timeout=TIMEOUT).read()
        return op
    except urllib.error.HTTPError as exc:
        # The code, not just the class. "HTTPError" told us nothing: 403 is
        # NSE refusing this client, 429 is asking us to wait, and 503 is
        # NSE being down - three different problems with three different
        # answers, all previously reported as the same word.
        sys.stderr.write("nse handshake refused: HTTP %s\n" % exc.code)
        return None
    except Exception as exc:
        sys.stderr.write("nse handshake failed: %s: %s\n"
                         % (type(exc).__name__, exc))
        return None


# Face value split: "From Rs 10/- To Rs 2/-", "From Re 1/- To Re 0.50"
_SPLIT = re.compile(r"from\s+rs?\.?\s*([\d.]+).*?to\s+rs?e?\.?\s*([\d.]+)",
                    re.I | re.S)
# Bonus: "Bonus 1:1", "Bonus issue 3:5"
_BONUS = re.compile(r"bonus.*?(\d+)\s*:\s*(\d+)", re.I | re.S)
_DIVIDEND = re.compile(r"dividend", re.I)


def classify(subject):
    """(kind, factor) for one NSE subject line. factor is None when the
    wording does not yield one with confidence."""
    s = (subject or "").strip()
    if not s:
        return "unknown", None

    m = _BONUS.search(s)
    if m:
        # a free shares for every b held -> (a+b) shares where there were b
        a, b = int(m.group(1)), int(m.group(2))
        if a > 0 and b > 0:
            return "bonus", round(b / float(a + b), 8)
        return "bonus", None

    if "split" in s.lower() or "sub-division" in s.lower():
        m = _SPLIT.search(s)
        if m:
            old, new = float(m.group(1)), float(m.group(2))
            # Face value falling from 10 to 2 means one share became five,
            # so a price from before the date is five times too large.
            if old > 0 and new > 0 and new < old:
                return "split", round(new / old, 8)
        return "split", None

    if _DIVIDEND.search(s):
        # Recorded, deliberately not adjusted - see the module docstring.
        return "dividend", None

    return "other", None


def _date(raw):
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime((raw or "").strip(), fmt).date()
        except ValueError:
            continue
    return None


def fetch():
    """Every equity corporate action NSE currently publishes."""
    op = _opener()
    if op is None:
        return []
    req = urllib.request.Request(API, headers=UA)
    body = op.open(req, timeout=TIMEOUT).read()
    rows = json.loads(body)
    if not isinstance(rows, list):
        sys.stderr.write("unexpected payload shape: %s\n" % type(rows).__name__)
        return []
    out = []
    for r in rows:
        sym = (r.get("symbol") or "").strip().upper()
        # NSE has moved this field's name more than once; take whichever
        # of them is present rather than failing on the vintage.
        ex = _date(r.get("exDate") or r.get("ex_dt") or r.get("exdate"))
        subject = (r.get("subject") or r.get("purpose") or "").strip()
        if not sym or not ex:
            continue
        kind, factor = classify(subject)
        out.append({"symbol": sym, "ex_date": ex.isoformat(), "kind": kind,
                    "factor": factor, "subject": subject})
    out.sort(key=lambda a: (a["ex_date"], a["symbol"]))
    return out


def load():
    """What we hold, as {symbol: [action, ...]} with the newest last.

    Missing file is not an error - it is a box where this has not run yet,
    and every caller has to treat "no actions" as normal anyway.
    """
    if not os.path.exists(STORE):
        return {}
    try:
        with open(STORE, encoding="utf-8") as f:
            rows = json.load(f)
    except (OSError, ValueError) as exc:
        sys.stderr.write("corp_actions unreadable (%s) - treating as empty\n"
                         % type(exc).__name__)
        return {}
    by_sym = {}
    for a in rows:
        by_sym.setdefault(a["symbol"], []).append(a)
    for v in by_sym.values():
        v.sort(key=lambda a: a["ex_date"])
    return by_sym


def save(rows):
    """Merge into what is there, keyed on (symbol, ex_date, kind).

    Merged rather than replaced because NSE's window moves: today's call
    shows the next few weeks and drops what has passed, so a replace would
    quietly forget every action older than the current window - which is
    every action that has actually happened, the only ones that matter.
    """
    have = {}
    if os.path.exists(STORE):
        try:
            with open(STORE, encoding="utf-8") as f:
                for a in json.load(f):
                    have[(a["symbol"], a["ex_date"], a["kind"])] = a
        except (OSError, ValueError):
            pass
    added = sum(1 for a in rows
                if (a["symbol"], a["ex_date"], a["kind"]) not in have)
    for a in rows:
        have[(a["symbol"], a["ex_date"], a["kind"])] = a
    merged = sorted(have.values(), key=lambda a: (a["ex_date"], a["symbol"]))
    os.makedirs(os.path.dirname(STORE), exist_ok=True)
    tmp = STORE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=1)
    os.replace(tmp, STORE)
    return len(merged), added


def factor_for(actions, before_day, upto_day):
    """How much to scale prices at `before_day` to compare with `upto_day`.

    Every adjusting action strictly after before_day and on or before
    upto_day, multiplied together - two splits in a window compose, and
    handling only the first is a bug that appears once a decade and is
    impossible to spot.
    """
    f = 1.0
    for a in actions:
        if a.get("factor") is None:
            continue
        ex = a["ex_date"]
        if str(before_day) < ex <= str(upto_day):
            f *= a["factor"]
    return f


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true", help="print, write nothing")
    ap.add_argument("--show", metavar="SYMBOL")
    args = ap.parse_args()

    if args.show:
        acts = load().get(args.show.upper(), [])
        if not acts:
            print("%s: nothing recorded" % args.show.upper())
            return
        for a in acts:
            print("  %s  %-9s %-8s %s" % (
                a["ex_date"], a["kind"],
                "-" if a["factor"] is None else "%.6g" % a["factor"],
                a["subject"][:70]))
        return

    t0 = time.time()
    rows = fetch()
    if not rows:
        # Not fatal, and not silent. NSE's www host refuses datacentre IPs,
        # so this fails on the very boxes that need it - but bars_fetch
        # collects splits from Yahoo's own events block on every run, so
        # the archive is not left blind. This source is the richer one when
        # it answers (it names bonuses and face-value changes Yahoo omits)
        # and a supplement when it does not.
        print("NSE did not answer - splits still come from Yahoo's events "
              "block via bars_fetch. This source adds bonuses and face-value "
              "detail when it works.")
        return

    kinds = {}
    for a in rows:
        kinds[a["kind"]] = kinds.get(a["kind"], 0) + 1
    adjusting = [a for a in rows if a["factor"] is not None]
    # Counted and printed rather than logged quietly. These are the ones a
    # reader will silently get wrong, and the number is the only signal
    # that the parsing needs a case adding.
    unparsed = [a for a in rows
                if a["factor"] is None and a["kind"] in ("split", "bonus")]

    print("%d action(s) in %.0fs: %s" % (
        len(rows), time.time() - t0,
        ", ".join("%s %d" % kv for kv in sorted(kinds.items()))))
    print("  %d adjusting (split/bonus with a usable ratio)" % len(adjusting))
    for a in adjusting[:8]:
        print("    %s  %-12s %-6s x%.6g  %s" % (a["ex_date"], a["symbol"],
                                                a["kind"], a["factor"],
                                                a["subject"][:48]))
    if unparsed:
        print("  %d split/bonus WITHOUT a usable ratio - these stay "
              "unadjusted:" % len(unparsed))
        for a in unparsed[:8]:
            print("    %s  %-12s %s" % (a["ex_date"], a["symbol"],
                                        a["subject"][:60]))

    if args.dry:
        print("\n--dry: nothing written")
        return
    total, added = save(rows)
    print("\n%d stored (%d new) at %s" % (total, added, STORE))


if __name__ == "__main__":
    main()
