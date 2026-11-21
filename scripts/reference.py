#!/usr/bin/env python3
"""A slow, literal reference implementation of rules.md, written independently
of the Rust pipeline, for cross-checking on small generated datasets.

Usage: reference.py DATA_DIR OUT_DIR   (writes user_daily.csv and campaign_daily.csv)
compare.py accepts a folder holding those two CSVs.
"""
import calendar
import csv as _csv
import datetime as dt
import glob
import json
import os
import sys
from collections import defaultdict

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 30 * 60 * 1000
DIG = set(b"0123456789")


def read_lines(path):
    with open(path, "rb") as f:
        data = f.read()
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()
    return lines[1:]  # header


def fields(line, n):
    parts = line.split(b",")
    if len(parts) != n:
        return None
    out = []
    for p in parts:
        if b'"' in p:
            if len(p) >= 2 and p[:1] == b'"' and p[-1:] == b'"' and b'"' not in p[1:-1]:
                p = p[1:-1]
            else:
                return None
        out.append(p)
    return out


def digits(b):
    return len(b) > 0 and all(c in DIG for c in b)


def civil(b):
    # YYYY-MM-DDTHH:MM:SS
    if len(b) < 19 or b[4:5] != b"-" or b[7:8] != b"-" or b[10:11] != b"T" or b[13:14] != b":" or b[16:17] != b":":
        return None
    parts = [b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19]]
    if not all(digits(p) for p in parts):
        return None
    y, mo, d, h, mi, s = (int(p) for p in parts)
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or s > 59:
        return None
    if d < 1 or d > calendar.monthrange(y, mo)[1] if y >= 1 else True:
        return None
    days = (dt.date(y, mo, d) - dt.date(1970, 1, 1)).days
    return ((days * 24 + h) * 60 + mi) * 60_000 + s * 1000


def parse_ts(b):
    if digits(b):
        v = int(b)
        return v if v < 2**63 else None
    if len(b) == 20 and b[19:] == b"Z":
        return civil(b)
    if len(b) == 25 and b[19:20] in (b"+", b"-") and b[22:23] == b":":
        oh, om = b[20:22], b[23:25]
        if not (digits(oh) and digits(om)):
            return None
        oh, om = int(oh), int(om)
        if oh > 23 or om > 59:
            return None
        base = civil(b)
        if base is None:
            return None
        off = (oh * 60 + om) * 60_000
        return base - off if b[19:20] == b"+" else base + off
    return None


def main():
    data, out = sys.argv[1], sys.argv[2]
    m = json.load(open(os.path.join(data, "manifest.json")))
    ws, we = parse_ts(m["window_start"].encode()), parse_ts(m["window_end"].encode())

    cat_of = {}
    for line in read_lines(os.path.join(data, "products.csv")):
        f = fields(line, 2)
        if f:
            cat_of[f[0]] = f[1]
    channel_of = {}
    for line in read_lines(os.path.join(data, "campaigns.csv")):
        f = fields(line, 2)
        if f:
            channel_of[f[0]] = f[1]

    # Linked IDs
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for path in sorted(glob.glob(os.path.join(data, "links", "*.csv"))):
        for line in read_lines(path):
            f = fields(line, 2)
            if not f or not f[0] or not f[1]:
                continue
            a, b = find(f[0]), find(f[1])
            if a != b:
                parent[a] = b
    name_of_root = {}
    for x in list(parent):
        r = find(x)
        if r not in name_of_root or x < name_of_root[r]:
            name_of_root[r] = x

    def person(uid):
        if uid in parent:
            return name_of_root[find(uid)]
        return uid

    # Events
    kept = {}
    for path in sorted(glob.glob(os.path.join(data, "events", "*.csv"))):
        for line in read_lines(path):
            f = fields(line, 7)
            if not f or not f[0]:
                continue
            ing = parse_ts(f[3])
            if f[0] not in kept or ing < kept[f[0]][0]:
                kept[f[0]] = (ing, f)
    by_person = defaultdict(list)
    for ing, f in kept.values():
        eid, uid, tsb, _, typ, prod, price = f
        t = parse_ts(tsb)
        if not uid or t is None or not (ws <= t < we) or typ not in (b"view", b"cart", b"purchase") or not prod:
            continue
        p = 0
        if typ == b"purchase":
            if not digits(price):
                continue
            p = int(price)
            if not (1 <= p <= 10_000_000):
                continue
        if ing - t > DAY:
            continue
        by_person[person(uid)].append((t, typ == b"purchase", p, cat_of.get(prod, b"unknown")))

    # Clicks
    clicks = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(data, "clicks", "*.csv"))):
        for line in read_lines(path):
            f = fields(line, 4)
            if not f or not f[0]:
                continue
            cid, uid, camp, tsb = f
            t = parse_ts(tsb)
            if not uid or t is None or not (ws - WEEK <= t < we) or camp not in channel_of:
                continue
            clicks[person(uid)].append((t, cid, camp))

    user = {}
    campaign = defaultdict(lambda: [0, 0, 0, 0])
    for pid, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        for e in evs:
            if sessions and e[0] - sessions[-1][-1][0] <= GAP:
                sessions[-1].append(e)
            else:
                sessions.append([e])
        cl = clicks.get(pid, [])
        for s in sessions:
            start = s[0][0]
            day = dt.date(1970, 1, 1) + dt.timedelta(days=start // DAY)
            rev = sum(e[2] for e in s if e[1])
            purch = any(e[1] for e in s)
            u = user.setdefault((pid, day), [0, 0, 0, defaultdict(int)])
            u[0] += 1
            u[1] += len(s)
            u[2] += rev
            for e in s:
                if e[1]:
                    u[3][e[3]] += e[2]
            q = [c for c in cl if start - WEEK <= c[0] <= start]
            if q:
                best = min(q, key=lambda c: (-c[0], c[1]))
                cred = best[2]
            else:
                cred = b"organic"
            k = campaign[(cred, day)]
            k[0] += 1
            k[1] += int(purch)
            k[2] += rev
            for camp in {c[2] for c in q}:
                campaign[(camp, day)][3] += 1

    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "user_daily.csv"), "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
        for (pid, day), (ns, ne, rv, cats) in user.items():
            top = ""
            if cats:
                top = min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0].decode()
            w.writerow([pid.decode(), day.isoformat(), ns, ne, rv, top])
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f)
        w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents",
                    "assisted_sessions"])
        for (camp, day), v in campaign.items():
            ch = b"organic" if camp == b"organic" else channel_of[camp]
            w.writerow([camp.decode(), ch.decode(), day.isoformat(), *v])


if __name__ == "__main__":
    main()
