#!/usr/bin/env python3
"""Mutation fuzzer: the binary against scripts/reference.py on hostile data.

Every round writes a random dataset aimed at the rules' sharp edges, runs the
binary on it (with a random NCPU) and the reference, and diffs the two with
compare.py. It must pass before any speed change is merged:

    python3 scripts/fuzz.py target/release/shopping-sessions            # 200 small rounds + 4 large
    python3 scripts/fuzz.py BIN --rounds 50 --large 2 --seed 7 --keep /tmp/fz

What a dataset holds:
- IDs: plain 1-10 chars of [0-9A-Z_a-z] (the packed fast path) next to "odd"
  ones: 11 to 300 chars, spaces at either end, `-`, tabs, `\\r`, non-ASCII,
  emoji, Unicode digits, and case/space variants of other IDs.
- Links: trees, long chains, self-links, repeats either way round, empty
  sides, IDs linked but never used (and sometimes the smallest of a person).
- Events: duplicate event_ids with other ingest_ts (some kept copies are
  invalid), exact duplicate lines across files, timestamps in all three forms
  at the window edges (F3 with many leading zeros, out-of-range F3), every bad
  form, gaps of exactly 30:00.000/30:00.001, equal ts, late events at exactly
  24 h and 24 h + 1 s, prices like 0250, 10000001, 2^64+1, Unicode digits.
- Clicks: at exactly 7 days, 7 days + 1 ms, at the session start, 1 ms after,
  same-ms pairs whose click_ids differ in case or spacing, unknown campaigns,
  the click window's edges.
- Lines: quotes around fields, stray quotes, wrong field counts, empty lines,
  `\\r\\n` endings, no final newline, header-only files, many files.
"""
import argparse
import datetime as dt
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
ALNUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
ODD = [" ", "-", ".", "é", "用", "\t", "\r", "~", "\U0001F600", "٢", "/", "+", " "]


def plain(r, lo=1, hi=10):
    return "".join(r.choice(ALNUM) for _ in range(r.randint(lo, hi)))


def odd(r, near=None):
    k = r.random()
    if near and k < 0.3:
        v = r.choice([" " + near, near + " ", near.swapcase(), near + "\r", near[:-1] or "0", near + "0"])
        return v if v != near else near + "_"
    if k < 0.45:
        return plain(r, 11, 40)
    if k < 0.55:
        return plain(r, 60, 300)
    if k < 0.65:
        return plain(r, 10, 10) + r.choice(ALNUM)  # 11 chars, one past packing
    s = plain(r, 1, 12)
    i = r.randint(0, len(s))
    return s[:i] + r.choice(ODD) + s[i:]


def iso(ms):
    return dt.datetime.fromtimestamp(ms // 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def fmt_ts(r, ms):
    """A valid rendering of ms (F1, F2 or F3)."""
    if ms % 1000 or r.random() < 0.3:
        z = "0" * r.choice([0, 0, 0, 1, 3, 8, 20]) if r.random() < 0.3 else ""
        return z + str(ms)
    if r.random() < 0.6:
        return iso(ms) + "Z"
    oh, om = r.randint(-10, 12), r.choice([0, 0, 30, 45])
    if oh == 0 and r.random() < 0.5:
        sign = "-"  # -00:xx
    else:
        sign = "+" if oh >= 0 else "-"
    off = (abs(oh) * 60 + om) * 60_000
    local = ms + off if sign == "+" else ms - off
    return iso(local) + f"{sign}{abs(oh):02d}:{om:02d}"


BAD_TS = [
    "", "2026-10-03T23:58:10z", "2026-10-03t23:58:10Z", "2026-10-03 23:58:10Z", "2026-10-03T23:58:10",
    "2026-10-03T23:58:10.000Z", "2026-13-01T00:00:00Z", "2026-02-30T00:00:00Z", "2027-02-29T00:00:00Z",
    "2026-10-03T24:00:00Z", "2026-10-03T23:60:00Z", "2026-10-03T23:58:60Z", "2026-10-03T23:58:10+24:00",
    "2026-10-03T23:58:10+05:60", "2026-10-03T23:58:10+0200", "1791071890000.0", "1.79e12", "-5",
    "١٧٩١", " 1791071890000", "1791071890000 ", "99999999999999999999", "18446744073709551617",
    "2026-1-03T23:58:10Z", "+1791071890000",
]


def bad_ts(r, ms):
    if r.random() < 0.5:
        return r.choice(BAD_TS)
    good = iso(ms)
    return r.choice([good + "z", good.replace("T", " ") + "Z", good + ".5Z", good, good + "+0100", good + "Z\r"])


def price(r, typ):
    if typ != "purchase" and r.random() < 0.7:
        return r.choice(["", "", "0", "abc", str(r.randint(1, 5000))])
    if r.random() < 0.8:
        return str(r.randint(100, 50_000))
    return r.choice(["1", "10000000", "10000001", "0", "0250", "", " 5", "5 ", "12.5", "-3", "٢٥٠",
                     "+5", "18446744073709551617", "000000000000000000000042", "99999999999999999999", "1000\r"])


def mutate_line(r, fields, rate, cr=True):
    """Render fields as one CSV line, sometimes damaged. Damage either makes
    the line malformed (it is dropped) or keeps its values, except `\r`
    endings, which change the last value: events get those before they are
    copied (cr=False here), since copies with equal ingest_ts are identical."""
    f = list(fields)
    x = r.random()
    if x < rate * 0.35:
        for i in range(len(f)):
            if r.random() < 0.4:
                f[i] = '"' + f[i] + '"'
    elif x < rate * 0.45:
        i = r.randrange(len(f))
        j = r.randint(0, len(f[i]))
        f[i] = f[i][:j] + '"' + f[i][j:]  # stray quote: malformed
    elif x < rate * 0.55:
        f.append(r.choice(["", "x"]))  # extra field: malformed
    elif x < rate * 0.62:
        f.pop(r.randrange(len(f)))
    elif x < rate * 0.70 and cr:
        f[-1] += "\r"
    elif x < rate * 0.73:
        return ""  # empty line
    elif x < rate * 0.76:
        i = r.randrange(len(f))
        f[i] = '"' + f[i] + '"x'
    return ",".join(f)


def write_split(r, d, name, header, lines, big=False):
    d.mkdir(parents=True, exist_ok=True)
    nfiles = 1 if big and r.random() < 0.5 else r.randint(1, 6)
    buckets = [[] for _ in range(nfiles)]
    for ln in lines:
        buckets[r.randrange(nfiles)].append(ln)
    if nfiles > 1 and r.random() < 0.5:
        buckets.append([])  # a header-only file
    for i, b in enumerate(buckets):
        body = header + "\n" + "\n".join(b)
        if b and r.random() < 0.75:
            body += "\n"  # else: no final newline
        elif not b:
            body = header + ("\n" if r.random() < 0.8 else "")
        (d / f"part-{i:05d}.csv").write_text(body, encoding="utf-8", newline="")


def dataset(r, out, n_events, odd_rate, mut_rate):
    ws = int(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp() * 1000) + r.randint(0, 900) * DAY
    ws -= ws % DAY
    we = ws + r.choice([1, 2, 3, 7, 14]) * DAY
    (out).mkdir(parents=True)
    (out / "manifest.json").write_text(json.dumps({"window_start": iso(ws) + "Z", "window_end": iso(we) + "Z"}))

    cats = r.sample(["garden", "apparel", "toys", "a", "unknowm", "unknownx", "zzz", "Books", "_x", "b c"], 6)
    prods = [f"p{i}" for i in range(1, 41)]
    (out / "products.csv").write_text("product_id,category\n" + "".join(f"{p},{r.choice(cats)}\n" for p in prods))
    camps = [f"c_{i}" for i in range(1, 16)]
    (out / "campaigns.csv").write_text("campaign_id,channel\n" + "".join(
        f"{c},{r.choice(['email', 'search', 'social', 'display'])}\n" for c in camps))

    used = set()

    def new_id(near=None, force_plain=False):
        while True:
            v = plain(r) if force_plain or r.random() >= odd_rate else odd(r, near)
            if v and "," not in v and '"' not in v and "\n" not in v and v not in used:
                used.add(v)
                return v

    # People: each a few IDs linked in a tree, some as long chains.
    n_people = max(3, n_events // 12)
    people = []
    links = []
    for _ in range(n_people):
        k = r.choices([1, 2, 3, 5, 12], [45, 30, 13, 8, 4])[0]
        if r.random() < 0.01:
            k = r.randint(30, 120)
        ids = [new_id()]
        for _ in range(k - 1):
            ids.append(new_id(r.choice(ids) if r.random() < 0.2 else None))
        for i in range(1, len(ids)):
            j = i - 1 if k > 20 else r.randrange(i)
            links.append([ids[i], ids[j]] if r.random() < 0.5 else [ids[j], ids[i]])
        if r.random() < 0.05:
            ghost = new_id(ids[0])
            links.append([ghost, r.choice(ids)])  # linked, never used
        people.append(ids)
    for _ in range(max(1, len(links) // 10)):
        a = r.choice(links)
        links.append(r.choice([a[::-1], a, [a[0], a[0]], [a[0], ""], ["", a[1]]]))
    # A few shared devices join several people into one.
    for _ in range(r.randint(0, 2)):
        dev = new_id()
        for p in r.sample(people, min(len(people), r.randint(2, 6))):
            links.append([dev, r.choice(p)])
    r.shuffle(links)

    # Events: per person, clusters of events with edge-case gaps.
    events, clicks = [], []
    eids = []
    ingests = {}  # event_id -> ingest_ts values used: equal ingest means identical copy
    cids = []
    starts = [ws, ws + 1, we - 1, we - GAP, ws - 1, we]
    n = 0
    while n < n_events:
        p = r.choice(people)
        t = r.choice(starts) if r.random() < 0.1 else r.randint(ws - DAY // 4, we + DAY // 8)
        sess_start = t
        for _ in range(r.choices([1, 2, 4, 8, 20], [30, 25, 25, 15, 5])[0]):
            uid = p[0] if r.random() < 0.5 else r.choice(p)
            typ = r.choices(["view", "cart", "purchase", "View", "purchase ", "", "buy"],
                            [55, 20, 20, 1, 1, 2, 1])[0]
            prod = r.choice(prods) if r.random() < 0.9 else r.choice(["p999", "", " p1", "P1", "p1\r"])
            # ingest: whole seconds; sometimes exactly 24 h, 24 h + 1 s, or before ts
            lag = r.choice([0, 1000, 60_000, DAY, DAY + 1000, DAY - 1000, -3_600_000, 2 * DAY, r.randint(0, DAY)])
            ing = t + lag
            if t % 1000 and lag in (DAY, DAY + 1000, DAY - 1000):
                ing = t - t % 1000 + lag + (1000 if lag == DAY + 1000 else 0)
            ing -= ing % 1000
            if r.random() < 0.08 and eids:
                eid = r.choice(eids)  # duplicate id: another ingest_ts
                ing += r.choice([-1000, 1000, 60_000, -DAY])
                while ing in ingests[eid]:
                    ing += 1000
            else:
                eid = new_id(force_plain=r.random() < 0.1)
                eids.append(eid)
                ingests[eid] = set()
            ingests[eid].add(ing)
            tsf = fmt_ts(r, t) if r.random() > 0.01 else bad_ts(r, t)
            uidf = uid if r.random() > 0.005 else ""
            ev = [eid, uidf, tsf, iso(ing) + "Z", typ, prod, price(r, typ)]
            if r.random() < mut_rate * 0.07:
                ev[-1] += "\r"  # a CRLF line ending, on every copy
            events.append(ev)
            if r.random() < 0.03:
                events.append(list(ev))  # identical copy (same ingest)
            n += 1
            t += r.choice([0, 0, 1, 1000, 60_000, GAP - 1, GAP, GAP + 1, GAP, r.randint(0, 3 * GAP), 6 * GAP,
                           DAY // 2])
        # Clicks around this cluster's start.
        for _ in range(r.choices([0, 1, 2, 4], [40, 30, 20, 10])[0]):
            ct = sess_start - r.choice([0, 1, -1, WEEK, WEEK + 1, WEEK - 1, r.randint(0, WEEK), r.randint(0, DAY)])
            if r.random() < 0.05:
                ct = r.choice([ws - WEEK, ws - WEEK - 1, we, we - 1])
            uid = r.choice(p)
            camp = r.choice(camps) if r.random() < 0.93 else r.choice(["c_999", "", " c_1", "C_1", "organic"])
            if cids and r.random() < 0.05:
                c = list(r.choice(cids))  # identical duplicate click
            else:
                cid = new_id(force_plain=r.random() < 0.2)
                c = [cid, uid, camp, fmt_ts(r, ct) if r.random() > 0.01 else bad_ts(r, ct)]
                cids.append(c)
                if r.random() < 0.15:  # same-ms rival with a near-identical click_id
                    c2 = [new_id(cid), r.choice(p), r.choice(camps), fmt_ts(r, ct)]
                    cids.append(c2)
                    clicks.append(c2)
            clicks.append(c)

    r.shuffle(events)
    big = n_events >= 50_000
    write_split(r, out / "events", "events", "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
                [mutate_line(r, e, mut_rate, cr=False) for e in events], big)
    write_split(r, out / "clicks", "clicks", "click_id,user_id,campaign_id,ts",
                [mutate_line(r, c, mut_rate) for c in clicks], big)
    write_split(r, out / "links", "links", "user_id_a,user_id_b",
                [mutate_line(r, l, mut_rate) for l in links], False)


def run_round(args, seed, n_events, odd_rate, mut_rate, work):
    r = random.Random(seed)
    data, got, ref = work / "data", work / "out", work / "ref"
    for d in (data, got, ref):
        shutil.rmtree(d, ignore_errors=True)
    dataset(r, data, n_events, odd_rate, mut_rate)
    ncpu = r.choice(["1", "2", "3", "8"])
    env = dict(os.environ, NCPU=ncpu)
    p = subprocess.run([args.binary, str(data), str(got)], env=env, capture_output=True, text=True)
    if p.returncode != 0:
        return f"binary exit {p.returncode}: {p.stderr[-500:]}"
    p = subprocess.run([sys.executable, str(HERE / "reference.py"), str(data), str(ref)], capture_output=True,
                       text=True)
    if p.returncode != 0:
        return f"reference failed: {p.stderr[-500:]}"
    p = subprocess.run([sys.executable, str(HERE / "compare.py"), str(got), str(ref)], capture_output=True,
                       text=True)
    if p.stdout.strip().endswith("MATCH") and p.returncode == 0:
        return None
    return f"NCPU={ncpu}\n{p.stdout[-3000:]}{p.stderr[-1000:]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("binary")
    ap.add_argument("--rounds", type=int, default=200, help="small rounds (300-3000 events)")
    ap.add_argument("--large", type=int, default=4, help="large rounds (150k-400k events, files over a chunk)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--start", type=int, default=0, help="first small round (to replay a failure)")
    ap.add_argument("--keep", help="keep the first failing dataset here and stop")
    args = ap.parse_args()
    args.binary = os.path.abspath(args.binary)
    work = Path(tempfile.mkdtemp(prefix="fuzz-"))
    fails = 0
    plan = [("small", i) for i in range(args.start, args.rounds)] + [("large", i) for i in range(args.large)]
    try:
        plan = [("small", i) for i in range(args.start, args.rounds)] + [("large", i) for i in range(args.large)]
        for kind, i in plan:
            seed = args.seed * 1_000_003 + i * 2 + (kind == "large")
            r = random.Random(seed ^ 0x5EED)
            if kind == "small":
                n, odd_rate, mut = r.randint(300, 3000), r.choice([0.05, 0.3, 0.8]), r.choice([0.0, 0.05, 0.3])
            else:
                n, odd_rate, mut = r.randint(150_000, 400_000), r.choice([0.1, 0.4]), r.choice([0.01, 0.05])
            err = run_round(args, seed, n, odd_rate, mut, work)
            tag = f"{kind} #{i} seed {seed} events {n} odd {odd_rate} mut {mut}"
            if err:
                fails += 1
                print(f"FAIL {tag}\n{err}", flush=True)
                if args.keep:
                    shutil.rmtree(args.keep, ignore_errors=True)
                    shutil.copytree(work / "data", args.keep)
                    print(f"dataset kept in {args.keep}")
                    break
            elif kind == "large" or i % 25 == 0:
                print(f"ok   {tag}", flush=True)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    total = len(plan)
    print(f"\n{'FUZZ PASS' if not fails else 'FUZZ FAIL'}: {fails} failing of {total} rounds")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
