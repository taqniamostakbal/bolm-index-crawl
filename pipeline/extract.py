#!/usr/bin/env python3
"""Extract one discovery chunk of Common Crawl captures into docs JSONL.

Input : a chunk JSONL file emitted by discover.py; each line is
        {"url","filename","offset","length", ...} — a WARC range pointer.
Flow  : range-GET https://data.commoncrawl.org/<filename> with a Range
        header -> gzip-member decompress -> split WARC + HTTP headers ->
        dechunk if needed -> decode charset -> trafilatura extraction
        (stdlib HTML fallback) -> canon dedupe + spam filters.
Output: docs JSONL, one record per line:
        {"url","title","text","host","cat","source",
         "warc":{"filename","offset","length"}}
        plus "description" when a meta description exists (distill.py
        turns it into the snippet).

If HF_TOKEN is set (and --hf-repo or $HF_DATASET_REPO is given), the
output file is uploaded to that HF Dataset repo in the same job — chunk
outputs must land somewhere durable immediately (GitHub artifacts are
transit only). Without HF_TOKEN the upload is skipped cleanly.

Usage:
  python3 pipeline/extract.py --chunk chunks/chunk-00000.jsonl \
      --out docs/chunk-00000.jsonl [--workers 8] [--min-text 300]
"""
import argparse
import gzip
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canon import canon_key, host_key  # noqa: E402
from spam import spam_check  # noqa: E402
import common  # noqa: E402

CC_DATA = "https://data.commoncrawl.org/"
UA = {"User-Agent": "bolm-index-crawl/1.0 (+https://github.com/"
      "taqniamostakbal/bolm-index-crawl)"}
MAX_TEXT = 20000

try:
    import requests
except ImportError:  # minimal environments: urllib fallback below
    requests = None


# ------------------------------------------------------------ HTTP fetching

def _get_range(url, offset, length, timeout=40):
    """Return raw bytes for one WARC record range, or raise."""
    headers = {**UA, "Range": f"bytes={offset}-{offset + length - 1}"}
    if requests is not None:
        r = requests.get(url, headers=headers, timeout=timeout)
        try:
            if r.status_code not in (200, 206):
                raise IOError(f"warc http {r.status_code}")
            return r.content
        finally:
            r.close()
    import urllib.request
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _dechunk(data):
    out, pos = bytearray(), 0
    while True:
        idx = data.find(b"\r\n", pos)
        if idx == -1:
            break
        try:
            size = int(data[pos:idx].strip().split(b";")[0], 16)
        except Exception:
            return bytes(out) if out else data
        if size == 0:
            break
        out += data[idx + 2: idx + 2 + size]
        pos = idx + 2 + size + 2
    return bytes(out)


def warc_record_to_html(raw):
    """gzip WARC record bytes -> decoded HTML string (or None).
    Mirrors the proven stage-2 logic in code/crawl.py:cc_fetch_record."""
    try:
        data = gzip.decompress(raw)
    except Exception:
        data = raw
    for sep in (b"\r\n\r\n", b"\n\n"):  # strip WARC headers
        idx = data.find(sep)
        if idx != -1:
            data = data[idx + len(sep):]
            break
    charset, hdr_text = "utf-8", ""
    for sep in (b"\r\n\r\n", b"\n\n"):  # strip HTTP headers
        idx = data.find(sep)
        if idx != -1 and idx < 30000:
            hdr = data[:idx]
            if b"HTTP/" in hdr[:20] or b":" in hdr[:200]:
                hdr_text = hdr.decode("latin-1", errors="replace").lower()
                data = data[idx + len(sep):]
            break
    if "chunked" in hdr_text:
        data = _dechunk(data)
    if "gzip" in hdr_text or data[:2] == b"\x1f\x8b":
        try:
            data = gzip.decompress(data)
        except Exception:
            pass
    m = re.search(r"charset=([\w\-]+)", hdr_text)
    if m:
        charset = m.group(1)
    mm = re.search(rb'charset=["\']?([\w\-]+)', data[:4096].lower())
    if mm:
        charset = mm.group(1).decode("latin-1")
    try:
        html = data.decode(charset, errors="replace")
    except Exception:
        html = data.decode("utf-8", errors="replace")
    low = html.lower()
    if "<html" not in low and "<body" not in low and "<title" not in low:
        return None
    return html


def fetch_html(rec, retries=3):
    url = CC_DATA + rec["filename"]
    off, ln = int(rec["offset"]), int(rec["length"])
    last = None
    for attempt in range(retries):
        try:
            return warc_record_to_html(_get_range(url, off, ln)), None
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(2 + attempt * 3)
    return None, last


# --------------------------------------------------------------- extraction

_META_DESC = re.compile(
    r'<meta[^>]+(?:name|property)=["\'](?:description|og:description)["\']'
    r'[^>]+content=["\']([^"\']+)["\']', re.I)
_TITLE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)


def extract_text(html, url):
    """(title, text, description) via trafilatura; stdlib fallback."""
    description = ""
    m = _META_DESC.search(html[:60000])
    if m:
        description = re.sub(r"\s+", " ", m.group(1)).strip()[:500]
    try:
        import trafilatura
        text = trafilatura.extract(
            html, url=url, include_comments=False, include_tables=False,
            favor_recall=True) or ""
        meta = trafilatura.extract_metadata(html, default_url=url)
        title = (meta.title if meta and meta.title else "") or ""
        if meta and meta.description and not description:
            description = meta.description.strip()[:500]
    except Exception:
        text, title = "", ""
    if len(text) < 200:  # fallback: crude tag strip (stage-2 style)
        from html.parser import HTMLParser

        class _P(HTMLParser):
            skip_tags = {"script", "style", "nav", "header", "footer",
                         "aside", "noscript", "iframe", "svg", "form"}

            def __init__(self):
                super().__init__(convert_charrefs=True)
                self.skip, self.parts, self.tparts = 0, [], []
                self.in_title = False

            def handle_starttag(self, tag, attrs):
                if tag in self.skip_tags:
                    self.skip += 1
                if tag == "title":
                    self.in_title = True

            def handle_endtag(self, tag):
                if tag in self.skip_tags and self.skip > 0:
                    self.skip -= 1
                if tag == "title":
                    self.in_title = False

            def handle_data(self, data):
                if self.in_title:
                    self.tparts.append(data)
                elif self.skip == 0 and data.strip():
                    self.parts.append(data.strip())

        p = _P()
        try:
            p.feed(html)
        except Exception:
            pass
        fb_text = re.sub(r"\s+", " ", " ".join(p.parts)).strip()
        if len(fb_text) > len(text):
            text = fb_text
        if not title:
            title = re.sub(r"\s+", " ", " ".join(p.tparts)).strip()
    if not title:
        m = _TITLE.search(html[:60000])
        if m:
            title = re.sub(r"\s+", " ", m.group(1)).strip()
    return title[:300], re.sub(r"\s+", " ", text).strip()[:MAX_TEXT], description


# -------------------------------------------------------------------- main

def process_chunk(chunk_path, out_path, workers=8, min_text=300):
    with open(chunk_path, encoding="utf-8") as f:
        recs = [json.loads(l) for l in f if l.strip()]
    stats = {"captures": len(recs), "kept": 0, "fetch_fail": 0,
             "short": 0, "dupe": 0, "spam": 0}
    seen, lock = set(), threading.Lock()
    docs = []

    def work(rec):
        html, err = fetch_html(rec)
        if html is None:
            return ("fetch_fail", None)
        title, text, desc = extract_text(html, rec.get("url", ""))
        if len(text) < min_text:
            return ("short", None)
        url = rec.get("url", "")
        excluded, _r, _lang = spam_check(url, title, text)
        if excluded:
            return ("spam", None)
        doc = {"url": url, "title": title, "text": text,
               "host": host_key(url),
               "cat": rec.get("cat") or "general",
               "source": "commoncrawl",
               "warc": {"filename": rec["filename"],
                        "offset": int(rec["offset"]),
                        "length": int(rec["length"])}}
        if desc:
            doc["description"] = desc
        if rec.get("timestamp"):
            doc["captured"] = rec["timestamp"]
        return ("ok", doc)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work, r): r for r in recs}
        for fut in as_completed(futs):
            status, doc = fut.result()
            if status != "ok":
                stats[status if status in stats else "fetch_fail"] += 1
                continue
            ck = canon_key(doc["url"])
            with lock:
                if ck in seen:
                    stats["dupe"] += 1
                    continue
                seen.add(ck)
                docs.append(doc)
                stats["kept"] += 1
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for d in docs:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--chunk", required=True, help="chunk JSONL from discover.py")
    ap.add_argument("--out", required=True, help="docs JSONL to write")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--min-text", type=int, default=300)
    ap.add_argument("--hf-repo", default=os.environ.get("HF_DATASET_REPO", ""),
                    help="HF Dataset repo id (default: $HF_DATASET_REPO)")
    ap.add_argument("--hf-path", default="",
                    help="path in repo (default: docs/<out basename>)")
    args = ap.parse_args(argv)

    stats = process_chunk(args.chunk, args.out, args.workers, args.min_text)
    print(f"extract: {stats} -> {args.out}")
    hf_path = args.hf_path or f"docs/{os.path.basename(args.out)}"
    common.maybe_upload_hf(args.out, args.hf_repo, hf_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
