#!/usr/bin/env python3
"""Distill stage-2-shaped docs JSONL into the Phase-3 distilled contract.

Input : one or more docs JSONL files, either stage-2 shape
        {url,title,text,host,category|cat,tier,fetched_at,...}
        or extract.py output {url,title,text,host,cat,source,warc,...},
        or already-distilled records (re-distillation is idempotent).
Output: distilled JSONL, one record per line, exactly:
        {"url","title","host","tier","shard","cat","lang","captured",
         "text","snippet"}
        - text capped at 6000 chars
        - snippet = meta description or first ~240 chars of text
        - tier/shard assigned by host rules (see common.py)
        - per-host caps (officials 400, press 300, general 60 by default)
        - Moroccan-first sort: tier asc, then host, stable within host

Stdlib + the copied stage-2 modules only (normalize/canon/spam), so it
runs anywhere, including the project venv with no third-party deps.

Usage:
  python3 pipeline/distill.py docs.jsonl [more.jsonl ...] \
      --out out/distilled.jsonl [--per-shard] \
      [--cap-official 400] [--cap-press 300] [--cap-general 60] \
      [--cap-arab 60] [--min-text 200] [--no-spam-filter]
"""
import argparse
import collections
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canon import canon_key, content_fp, host_key  # noqa: E402
from normalize import normalize  # noqa: E402
from spam import spam_check  # noqa: E402
import common  # noqa: E402

TEXT_CAP = 6000


def iter_docs(paths):
    for path in paths:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def distill(paths, caps, min_text=200, spam_filter=True, registry=None):
    """Return (records, stats). caps = dict with keys official/press/
    general/arab giving the per-host cap for each class."""
    seen_canon, seen_fp = set(), set()
    per_host = collections.Counter()
    kept, stats = [], collections.Counter()
    for doc in iter_docs(paths):
        stats["input"] += 1
        url = (doc.get("url") or "").strip()
        text = (doc.get("text") or "").strip()
        if not url or len(text) < min_text:
            stats["dropped_short_or_nourl"] += 1
            continue
        host = host_key(url) or (doc.get("host") or "").strip().lower()
        if host.startswith("www."):
            host = host[4:]
        cat = doc.get("cat") or doc.get("category") or ""
        ck = canon_key(url)
        if ck in seen_canon:
            stats["dropped_canon_dupe"] += 1
            continue
        fp = content_fp(normalize(text))
        if fp in seen_fp:
            stats["dropped_content_dupe"] += 1
            continue
        title = (doc.get("title") or "").strip()
        lang = doc.get("lang") or ""
        if spam_filter:
            excluded, _reasons, detected = spam_check(url, title, text)
            if excluded:
                stats["dropped_spam"] += 1
                continue
            lang = lang or detected
        tier, shard = common.classify(host, cat or None, registry)
        if tier == 0:
            cap = caps["official"]
        elif shard == 2:
            cap = caps["press"]
        elif tier == 3:
            cap = caps["arab"]
        else:
            cap = caps["general"]
        if per_host[host] >= cap:
            stats["dropped_host_cap"] += 1
            continue
        seen_canon.add(ck)
        seen_fp.add(fp)
        per_host[host] += 1
        text = text[:TEXT_CAP]
        kept.append({
            "url": url,
            "title": title[:300],
            "host": host,
            "tier": tier,
            "shard": shard,
            "cat": cat or {0: "admin", 1: "education", 2: "press",
                           3: "commerce", 4: "general", 5: "arab-world"}[shard],
            "lang": lang or "other",
            "captured": doc.get("captured") or doc.get("fetched_at") or "",
            "text": text,
            "snippet": common.make_snippet(text, doc.get("description")),
        })
        stats["kept"] += 1
    # Moroccan-first: officials/established first; stable within host so
    # the source order (usually crawl order) is preserved per host.
    kept.sort(key=lambda r: (r["tier"], r["shard"], r["host"]))
    return kept, stats


def write_jsonl(records, path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("inputs", nargs="+", help="docs JSONL file(s)")
    ap.add_argument("--out", default="out/distilled.jsonl")
    ap.add_argument("--per-shard", action="store_true",
                    help="also write <out-stem>-shard<N>.jsonl per shard")
    ap.add_argument("--registry", default=None,
                    help="official registry JSON (default: seeds/)")
    ap.add_argument("--cap-official", type=int, default=400)
    ap.add_argument("--cap-press", type=int, default=300)
    ap.add_argument("--cap-general", type=int, default=60)
    ap.add_argument("--cap-arab", type=int, default=60)
    ap.add_argument("--min-text", type=int, default=200)
    ap.add_argument("--no-spam-filter", action="store_true")
    args = ap.parse_args(argv)

    registry = common.load_registry(args.registry)
    caps = {"official": args.cap_official, "press": args.cap_press,
            "general": args.cap_general, "arab": args.cap_arab}
    records, stats = distill(args.inputs, caps, min_text=args.min_text,
                             spam_filter=not args.no_spam_filter,
                             registry=registry)
    write_jsonl(records, args.out)
    if args.per_shard:
        stem, ext = os.path.splitext(args.out)
        by_shard = collections.defaultdict(list)
        for r in records:
            by_shard[r["shard"]].append(r)
        for shard in sorted(by_shard):
            write_jsonl(by_shard[shard], f"{stem}-shard{shard}{ext}")

    by_tier = collections.Counter(r["tier"] for r in records)
    by_shard = collections.Counter(r["shard"] for r in records)
    print(f"distill: {dict(stats)}")
    print(f"tiers : {dict(sorted(by_tier.items()))}")
    print(f"shards: {dict(sorted(by_shard.items()))}")
    print(f"wrote {len(records)} records -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
