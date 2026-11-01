#!/usr/bin/env python3
"""Discovery: find Common Crawl captures for *.ma + the distilled host
list, and emit 5,000-capture work chunks for extract.py.

Method (host-routes §4/§6, NO AWS/Athena): DuckDB reads the Common Crawl
URL Index (Apache Parquet) over HTTPS from data.commoncrawl.org using
httpfs range reads, filters to Moroccan hosts, and writes chunk files:

  chunks/chunk-00000.jsonl   {"url","filename","offset","length",
                              "timestamp","host","cat"}
  chunks/manifest.json       {"collection","chunk_size","total_captures",
                              "chunks":["00000", ...]}

Each chunk line is exactly the WARC range pointer extract.py needs:
(filename, offset, length) into https://data.commoncrawl.org/<filename>.

Correct-by-construction: this module requires `duckdb` (declared in
requirements.txt); it is exercised in the Discovery GitHub Actions
workflow, not on small dev machines. `--collection latest` resolves the
newest collection via index.commoncrawl.org/collinfo.json.

Usage:
  python3 pipeline/discover.py [--collection CC-MAIN-2026-43|latest] \
      [--chunk-size 5000] [--out-dir chunks] \
      [--registry seeds/official-registry.json] [--extra-hosts hosts.txt] \
      [--distilled-only] [--limit 0]
"""
import argparse
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

CC_INDEX_BASE = "https://data.commoncrawl.org/cc-index/table/cc-main/warc"
COLLINFO = "https://index.commoncrawl.org/collinfo.json"


def resolve_collection(collection):
    if collection and collection != "latest":
        return collection
    env = os.environ.get("CC_COLLECTION")
    if env:
        return env
    with urllib.request.urlopen(COLLINFO, timeout=30) as resp:
        colls = json.load(resp)
    if not colls:
        raise RuntimeError("collinfo.json returned no collections")
    return colls[0]["id"]


def load_host_list(registry_path, extra_hosts_path):
    """Distilled host list: registry domains, mapped to their category."""
    hosts = {}
    reg = common.load_registry(registry_path)
    for domain, entry in reg.items():
        hosts[domain] = entry.get("c") or "general"
    if extra_hosts_path and os.path.exists(extra_hosts_path):
        with open(extra_hosts_path, encoding="utf-8") as f:
            for line in f:
                h = line.strip().lower()
                if h and not h.startswith("#"):
                    hosts.setdefault(h, "general")
    return hosts


def build_query(collection, distilled_only, hosts, limit):
    """DuckDB SQL over the URL-index parquet (HTTPS, hive-partitioned).

    Column names follow the published CC URL Index schema:
    url, url_host_name, url_host_registered_domain, url_host_tld,
    warc_filename, warc_record_offset, warc_record_length,
    fetch_time, fetch_status, content_mime_detected.
    """
    glob = f"{CC_INDEX_BASE}/crawl={collection}/subset=warc/*.parquet"
    where = ["fetch_status = 200",
             "(content_mime_detected IS NULL OR "
             "content_mime_detected LIKE 'text/html%')"]
    if distilled_only:
        in_list = ", ".join("'" + h.replace("'", "''") + "'"
                            for h in sorted(hosts))
        where.append(f"url_host_registered_domain IN ({in_list})")
    else:
        where.append("url_host_tld = 'ma'")
    lim = f"LIMIT {int(limit)}" if limit else ""
    return f"""
        SELECT url,
               warc_filename       AS filename,
               warc_record_offset  AS "offset",
               warc_record_length  AS length,
               CAST(fetch_time AS VARCHAR) AS timestamp,
               url_host_name       AS host,
               url_host_registered_domain AS registered_domain
        FROM read_parquet('{glob}', hive_partitioning = true)
        WHERE {' AND '.join(where)}
        {lim}
    """


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--collection", default="latest",
                    help="CC collection id, or 'latest' (default); "
                         "$CC_COLLECTION is honoured for 'latest'")
    ap.add_argument("--chunk-size", type=int, default=5000)
    ap.add_argument("--out-dir", default="chunks")
    ap.add_argument("--registry", default=None)
    ap.add_argument("--extra-hosts", default="",
                    help="optional newline-delimited extra host list")
    ap.add_argument("--distilled-only", action="store_true",
                    help="only the registry/extra host list, not all *.ma")
    ap.add_argument("--limit", type=int, default=0,
                    help="cap total captures (0 = no cap; smoke runs)")
    args = ap.parse_args(argv)

    try:
        import duckdb
    except ImportError:
        print("error: duckdb is required (see requirements.txt); "
              "discovery runs in the GitHub Actions workflow", file=sys.stderr)
        return 2

    collection = resolve_collection(args.collection)
    hosts = load_host_list(args.registry, args.extra_hosts)
    cat_by_domain = dict(hosts)

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET http_retries = 5; SET http_timeout = 120;")
    sql = build_query(collection, args.distilled_only, hosts, args.limit)
    print(f"discover: collection={collection} distilled_only="
          f"{args.distilled_only} hosts={len(hosts)}")
    rel = con.execute(sql)

    os.makedirs(args.out_dir, exist_ok=True)
    chunk_ids, total, in_chunk = [], 0, 0
    fh = None

    def open_chunk():
        cid = f"{len(chunk_ids):05d}"
        chunk_ids.append(cid)
        return open(os.path.join(args.out_dir, f"chunk-{cid}.jsonl"),
                    "w", encoding="utf-8")

    fh = open_chunk()
    while True:
        rows = rel.fetchmany(1000)
        if not rows:
            break
        for url, filename, offset, length, ts, host, regdom in rows:
            rec = {"url": url, "filename": filename,
                   "offset": int(offset), "length": int(length),
                   "timestamp": ts or "", "host": host or "",
                   "cat": cat_by_domain.get(regdom or "", "general")}
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            total += 1
            in_chunk += 1
            if in_chunk >= args.chunk_size:
                fh.close()
                fh = open_chunk()
                in_chunk = 0
    if fh:
        fh.close()
    if in_chunk == 0 and chunk_ids:  # drop trailing empty chunk
        empty = os.path.join(args.out_dir, f"chunk-{chunk_ids[-1]}.jsonl")
        if os.path.exists(empty) and os.path.getsize(empty) == 0:
            os.remove(empty)
            chunk_ids.pop()
    manifest = {"collection": collection, "chunk_size": args.chunk_size,
                "total_captures": total, "chunks": chunk_ids,
                "distilled_only": bool(args.distilled_only)}
    with open(os.path.join(args.out_dir, "manifest.json"), "w",
              encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1)
    print(f"discover: {total} captures in {len(chunk_ids)} chunks "
          f"-> {args.out_dir}/ (manifest.json written)")

    # Land the chunks on the HF Dataset the same job when configured:
    # artifacts are only an in-run hand-off, never the archive.
    repo = os.environ.get("HF_DATASET_REPO", "")
    if os.environ.get("HF_TOKEN") and repo:
        try:
            from huggingface_hub import HfApi
            api = HfApi(token=os.environ["HF_TOKEN"])
            for cid in chunk_ids:
                api.upload_file(
                    path_or_fileobj=os.path.join(
                        args.out_dir, f"chunk-{cid}.jsonl"),
                    path_in_repo=f"chunks/{collection}/chunk-{cid}.jsonl",
                    repo_id=repo, repo_type="dataset")
            api.upload_file(
                path_or_fileobj=os.path.join(args.out_dir, "manifest.json"),
                path_in_repo=f"chunks/{collection}/manifest.json",
                repo_id=repo, repo_type="dataset")
            print(f"discover: chunks uploaded to HF dataset {repo}")
        except Exception as e:
            print(f"discover: HF upload failed ({type(e).__name__}: {e})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
