# Copied verbatim (logic unchanged) from the proven stage-2 pipeline:
#   ~/workspace/bolm-own-index/code/canon.py
# Productized into the Phase-3 crawl repo (taqniamostakbal/bolm-index-crawl) on 2026-11-01.
"""URL canonicalization + content fingerprinting for dedupe (v2).

Canonical KEY rules (used only for dedupe identity, display URL is kept
from the first-seen variant):
- scheme ignored (http == https), host lowercased, leading "www." stripped,
  default ports dropped, non-default ports kept.
- path: percent-escapes normalized (unreserved decoded, hex uppercased),
  duplicate slashes collapsed, a leading "/index.php" or "/index.html" or
  "/default.aspx" segment removed (men.gov.ma serves /index.php/<same page>
  as a duplicate route), trailing slash removed except root.
- query: tracking params dropped (utm_*, fbclid, gclid, dclid, msclkid,
  mc_cid, mc_eid, fbrefresh, ref, spm, _ga, gad_*, igshid); pagination
  params (page, pgno, p, offset, start) dropped -- page N of a listing is
  the same canonical doc for index purposes (kills the men.gov.ma ?page=
  and lematin bourse ?pgno= floods); remaining params sorted.
- fragment dropped.
"""
import hashlib
import re
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode, unquote

_DROP_PARAMS = {
    "fbclid", "gclid", "dclid", "msclkid", "mc_cid", "mc_eid", "fbrefresh",
    "ref", "spm", "_ga", "igshid", "page", "pgno", "p", "offset", "start",
}
_UNRESERVED = re.compile(r"%([0-9A-Fa-f]{2})")


def _norm_pct(path: str) -> str:
    def rep(m):
        ch = chr(int(m.group(1), 16))
        if ch.isalnum() or ch in "-._~":
            return ch
        return "%" + m.group(1).upper()
    return _UNRESERVED.sub(rep, path)


def canon_key(url: str) -> str:
    try:
        p = urlparse(url.strip())
        host = (p.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        port = p.port
        netloc = host
        if port and port not in (80, 443):
            netloc = f"{host}:{port}"
        path = _norm_pct(p.path or "/")
        path = re.sub(r"/+", "/", path)
        for seg in ("/index.php", "/index.html", "/default.aspx"):
            if path == seg:
                path = "/"
            elif path.startswith(seg + "/"):
                path = path[len(seg):]
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        q = []
        for k, v in parse_qsl(p.query, keep_blank_values=True):
            kl = k.lower()
            if kl in _DROP_PARAMS or kl.startswith("utm_") or kl.startswith("gad_"):
                continue
            q.append((k, v))
        q.sort()
        return urlunparse(("https", netloc, path, "", urlencode(q), ""))
    except Exception:
        return url.strip().lower()


def host_key(url: str) -> str:
    try:
        host = (urlparse(url.strip()).hostname or "").lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:
        return ""


_PUBLIC_SUFFIX2 = {"co.ma", "gov.ma", "ac.ma", "org.ma", "net.ma", "press.ma"}


def root_domain(url: str) -> str:
    """Registered domain (adm.co.ma for www.adm.co.ma, men.gov.ma for
    pw.men.gov.ma, telquel.ma for test.telquel.ma) — the level at which
    site chrome is shared."""
    h = host_key(url)
    parts = h.split(".")
    if len(parts) <= 2:
        return h
    last2 = ".".join(parts[-2:])
    if last2 in _PUBLIC_SUFFIX2 and len(parts) >= 3:
        return ".".join(parts[-3:])
    return last2


def path_template(url: str) -> str:
    """Path with volatile segments (pure digits, UUIDs, long hex) -> '*'."""
    try:
        path = urlparse(url.strip()).path or "/"
    except Exception:
        path = "/"
    segs = []
    for s in path.split("/"):
        if re.fullmatch(r"\d+", s or " "):
            segs.append("*")
        elif re.fullmatch(
                r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", s or ""):
            segs.append("*")
        elif re.fullmatch(r"[0-9a-fA-F]{16,}", s or ""):
            segs.append("*")
        else:
            segs.append(s)
    return "/".join(segs)


def content_fp(normalized_text: str) -> str:
    """Exact-duplicate fingerprint over normalized text (first 8000 chars)."""
    return hashlib.sha256(normalized_text[:8000].encode("utf-8")).hexdigest()
