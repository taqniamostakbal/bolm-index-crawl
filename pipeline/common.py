"""Shared host classification for the Bolm own-index Phase-3 pipeline.

Implements the SHARED CONTRACT tier/shard assignment used by distill.py
(and the tier hints written by extract.py):

  tier: 0=official, 1=established (press/banks/universities/key services),
        2=general Moroccan, 3=Arab-world
  shard: 0=officials-admin, 1=education-research (.ac.ma/universities),
         2=press-news, 3=commerce-services (banks/telecom/commerce/jobs),
         4=morocco-general (other .ma), 5=arab-world (non-.ma)

Tier precedence (first match wins):
  1. Host (or its registered domain) is in seeds/official-registry.json,
     or the host ends with .gov.ma                      -> tier 0
  2. Known press / bank / telecom host, any .ac.ma host,
     or input category in {press, banks, universities,
     banks_telecom_utility}                             -> tier 1
  3. Any other host ending in .ma                       -> tier 2
  4. Everything else (non-.ma)                          -> tier 3

Shard is assigned by category/host class, independently of tier:
registry category first, then input category, then host heuristics,
then the tier fallback (tier 3 -> shard 5, other .ma -> shard 4).
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canon import host_key, root_domain  # noqa: E402

# ---------------------------------------------------------------- registry

_REGISTRY_CACHE = None


def default_registry_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "seeds", "official-registry.json")


def load_registry(path=None):
    """Return (domain -> entry) for seeds/official-registry.json."""
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is not None and path is None:
        return _REGISTRY_CACHE
    p = os.path.normpath(path or default_registry_path())
    reg = {}
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            for entry in json.load(f):
                d = (entry.get("d") or "").strip().lower()
                if d:
                    reg[d] = entry
    if path is None:
        _REGISTRY_CACHE = reg
    return reg


def registry_entry(host, registry=None):
    reg = registry if registry is not None else load_registry()
    h = host_key("https://" + host) if "://" not in host else host_key(host)
    for cand in (h, root_domain("https://" + h)):
        if cand in reg:
            return reg[cand]
    return None


# ------------------------------------------------------- established hosts

PRESS_HOSTS = {
    "hespress.com", "lematin.ma", "yabiladi.com", "le360.ma", "telquel.ma",
    "leseco.ma", "leconomiste.com", "goud.ma", "kech24.com", "rue20.com",
    "assahraa.ma", "ahdath.info", "hesport.com", "elbotola.com",
    "challenge.ma", "lopinion.ma", "aujourdhui.ma", "albayane.ma",
    "mapexpress.ma", "maroc24.com", "bladi.net", "moroccoworldnews.com",
    "map.ma", "2m.ma", "snrt.ma", "lebrief.ma", "medias24.com",
    "lnt.ma", "fnh.ma", "maroc-hebdo.press.ma",
}
BANK_HOSTS = {
    "attijariwafabank.com", "cihbank.ma", "bankofafrica.ma", "bmci.ma",
    "sgmaroc.com", "creditagricole.ma", "cfg.com", "umniahbank.ma",
    "cdg.ma", "bkam.ma", "ammc.ma",
}
TELECOM_HOSTS = {"iam.ma", "orange.ma", "inwi.ma", "maroctelecom.ma"}
KEY_SERVICE_HOSTS = BANK_HOSTS | TELECOM_HOSTS | {
    "royalairmaroc.com", "oncf.ma", "oncf-voyages.ma", "onda.ma",
    "avito.ma", "mubawab.ma", "jumia.ma",
}

ESTABLISHED_CATS = {"press", "banks", "universities", "banks_telecom_utility"}

# Registry category -> shard
_REGISTRY_CAT_SHARD = {
    "admin": 0,
    "education": 1,
    "transport": 3, "health": 3, "social": 3, "jobs": 3,
    "prices": 3, "business": 3, "prayer": 3,
    "general": 4,
}
# Free-form input category -> shard
_CAT_SHARD = {
    "official": 0, "admin": 0,
    "education": 1, "universities": 1, "encyclopedia": 1,
    "press": 2, "news": 2,
    "transport": 3, "health": 3, "social": 3, "jobs": 3, "prices": 3,
    "business": 3, "prayer": 3, "banks": 3, "banks_telecom_utility": 3,
    "telecom": 3, "commerce": 3, "food": 3,
    "general": 4, "food_misc": 4, "communes": 4,
}

ARAB_TLDS = (".ma", ".dz", ".tn", ".ly", ".eg", ".sa", ".ae", ".qa", ".kw",
             ".om", ".bh", ".jo", ".lb", ".sy", ".iq", ".ye", ".ps", ".sd",
             ".mr", ".km", ".dj", ".so")


def _bare_host(host_or_url):
    h = host_or_url or ""
    return host_key(h) if "://" in h else host_key("https://" + h)


def assign_tier(host, cat=None, registry=None):
    h = _bare_host(host)
    if not h:
        return 3
    if registry_entry(h, registry) is not None or h.endswith(".gov.ma"):
        return 0
    rd = root_domain("https://" + h)
    if (rd in PRESS_HOSTS or rd in KEY_SERVICE_HOSTS or h.endswith(".ac.ma")
            or (cat or "").lower() in ESTABLISHED_CATS):
        return 1
    if h.endswith(".ma"):
        return 2
    return 3


def assign_shard(host, cat=None, tier=None, registry=None):
    h = _bare_host(host)
    if not h:
        return 5
    if tier is None:
        tier = assign_tier(h, cat, registry)
    if tier == 3:
        return 5
    entry = registry_entry(h, registry)
    if entry is not None:
        c = (entry.get("c") or "").lower()
        # A registry 'general' host that is really the news agency / a
        # broadcaster belongs with press, not morocco-general.
        rd = root_domain("https://" + h)
        if c == "general" and rd in PRESS_HOSTS:
            return 2
        if c in _REGISTRY_CAT_SHARD:
            return _REGISTRY_CAT_SHARD[c]
    c = (cat or "").lower()
    if c in _CAT_SHARD:
        return _CAT_SHARD[c]
    rd = root_domain("https://" + h)
    if h.endswith(".ac.ma"):
        return 1
    if rd in PRESS_HOSTS:
        return 2
    if rd in KEY_SERVICE_HOSTS:
        return 3
    if h.endswith(".gov.ma"):
        return 0
    return 4 if h.endswith(".ma") else 5


def classify(host, cat=None, registry=None):
    """Return (tier, shard) for a host per the shared contract."""
    tier = assign_tier(host, cat, registry)
    shard = assign_shard(host, cat, tier, registry)
    return tier, shard


# ----------------------------------------------------------------- helpers

def make_snippet(text, description=None, limit=240):
    """Snippet = meta description if provided, else first ~240 chars."""
    src = (description or "").strip() or (text or "").strip()
    return re.sub(r"\s+", " ", src)[:limit]


def maybe_upload_hf(local_path, repo_id, path_in_repo):
    """Upload one file to a HF Dataset repo. Skips cleanly (returns False)
    when HF_TOKEN is absent or huggingface_hub is unavailable; never raises."""
    token = os.environ.get("HF_TOKEN")
    if not token:
        print(f"[hf] HF_TOKEN not set - skipping upload of {local_path}")
        return False
    if not repo_id:
        print("[hf] no dataset repo configured - skipping upload")
        return False
    try:
        from huggingface_hub import HfApi
        HfApi(token=token).upload_file(
            path_or_fileobj=local_path, path_in_repo=path_in_repo,
            repo_id=repo_id, repo_type="dataset")
        print(f"[hf] uploaded {local_path} -> {repo_id}:{path_in_repo}")
        return True
    except Exception as e:  # upload must never kill a crawl job
        print(f"[hf] upload failed ({type(e).__name__}: {e}) - continuing")
        return False
