#!/usr/bin/env python3
"""Diagnostic: which HTTP access pattern survives data.commoncrawl.org
from a GitHub runner? Tries, in order: python urllib GET, then DuckDB
read_parquet variants (default / +UA / threads=1 / retries). Prints one
line per variant. Never raises."""
import sys, urllib.request

URL = ("https://data.commoncrawl.org/cc-index/table/cc-main/warc/"
       "crawl=CC-MAIN-2026-43/subset=warc/"
       "part-00052-6e44f0dc-08f9-465c-8a7f-b0da44e49f26.c000.zstd.parquet")

# V0: plain python range GET of the parquet footer area
try:
    req = urllib.request.Request(URL, headers={"Range": "bytes=0-1023"})
    with urllib.request.urlopen(req, timeout=30) as r:
        print("V0 python-urllib:", r.status, len(r.read()), "bytes")
except Exception as e:
    print("V0 python-urllib FAIL:", type(e).__name__, str(e)[:160])

import duckdb

def variant(name, setup):
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        for stmt in setup:
            con.execute(stmt)
        rows = con.execute(
            f"SELECT count(*) FROM read_parquet('{URL}') LIMIT 1").fetchall()
        print(name, "OK (full count scan):", rows)
    except Exception as e:
        # count(*) forces a full scan; a LIMIT-1 probe is the real test
        try:
            con2 = duckdb.connect()
            con2.execute("LOAD httpfs;")
            for stmt in setup:
                con2.execute(stmt)
            r = con2.execute(
                f"SELECT url FROM read_parquet('{URL}') LIMIT 1").fetchall()
            print(name, "OK limit1:", len(r), "row(s)")
        except Exception as e2:
            print(name, "FAIL:", type(e2).__name__, str(e2)[:200])
    finally:
        con.close()

variant("V1 duckdb-default", [])
variant("V2 duckdb-UA", [
    "SET http_user_agent='bolm-index-crawl/1.0 (+https://github.com/taqniamostakbal/bolm-index-crawl)';"])
variant("V3 duckdb-threads1", ["SET threads=1;"])
variant("V4 duckdb-retries", ["SET http_retries=10; SET http_timeout=120;"])
print("DIAG-DONE")
