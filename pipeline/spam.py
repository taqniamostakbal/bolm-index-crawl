# Copied verbatim (logic unchanged) from the proven stage-2 pipeline:
#   ~/workspace/bolm-own-index/code/spam.py
# Productized into the Phase-3 crawl repo (taqniamostakbal/bolm-index-crawl) on 2026-11-01.
"""Parasite/SEO spam detection for the .ma corpus (v2).

Signals (doc excluded if any HARD rule fires; reasons recorded):
1. casino-density: casino/gambling lexemes (EN/FI/DE) >= 3 hits AND
   >= 0.8% of tokens -> parasite gambling page.
2. foreign-language: dominant detected language not in {ar, fr, en, es}
   (cheap stopword classifier + Arabic-script ratio). A Moroccan corpus
   has no legitimate Finnish/German content; the specimen case is
   boutique.lematin.ma hosting Finnish casino pages.
3. subdomain-parasite: a non-standard subdomain (not www/ar/fr/en/app/m)
   of a press/official root whose content language mismatches the root's
   expected languages -> parasite section.
A corpus-level PARASITE-SECTION rule lives in build_v2.py: a full host
with >=5 docs and >=40% hard-spam is excluded wholesale (boutique.lematin.ma
is 91% casino). Staging subdomains (test.telquel.ma = 164 docs = ALL of
TelQuel in this corpus) are NOT excluded: the content is genuine
journalism, and content-fingerprint dedupe already prefers production
copies when both exist — excluding them would delete a whole source.

The two boutique.lematin.ma Finnish casino docs from the v1 corpus are
the acceptance specimens: both must be excluded.
"""
import re
import sys, os
import unicodedata

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canon import host_key

_CASINO = {
    "casino", "kasino", "kasinot", "nettikasino", "nettikasinot",
    "kasinobonus", "rahapelit", "rahapeli", "vedonlyönti", "vedonlyonti",
    "ilmaiskierrokset", "ilmaista", "pelata", "pelaaminen", "jackpot",
    "slots", "slot", "poker", "roulette", "blackjack", "gambling",
    "bookmaker", "talletus", "talletukset", "bonukset", "kierrokset",
    "voittoja", "rahapelaaminen", "livekasino", "kolikkopelit",
    "urheiluvedonlyönti", "bingo", "keno", "viagra", "cialis",
    "cassino", "cassinos", "apostas", "aposta", "jogos", "betting",
    "bookmakers", "freespins", "spin", "spins",
}
_FI = {
    "ja", "on", "ei", "että", "myös", "kun", "niin", "kuin", "ovat",
    "voi", "kanssa", "jälkeen", "tämä", "jotka", "sekä", "mutta",
    "jos", "jo", "uudet", "parhaat", "paras", "uusi", "pelaa",
    "pelaaminen", "pelata", "ovat", "suomen", "suomessa", "vuonna",
    "lisää", "kaikki", "myos", "etta",
}
_FR = {"le", "la", "les", "des", "de", "du", "et", "est", "dans", "pour",
       "qui", "sur", "plus", "avec", "vous", "nous", "aux", "une", "un",
       "au", "ce", "il", "elle", "sont", "être", "par", "pas", "ces"}
_EN = {"the", "of", "and", "to", "in", "is", "for", "with", "on", "are",
       "you", "your", "this", "that", "it", "as", "at", "by", "an", "be"}
_ES = {"el", "la", "los", "las", "de", "y", "en", "un", "una", "para",
       "con", "por", "del", "es", "se", "su", "al", "lo", "como"}
_DE = {"der", "die", "das", "und", "ist", "ein", "eine", "mit", "für",
       "von", "den", "im", "am", "sich", "auf", "nicht", "es", "sind"}
_AR_STOP = {"في", "من", "إلى", "على", "أن", "إن", "ما", "لا", "لم", "هو",
            "هي", "كان", "مع", "عن", "بعد", "قبل", "عند", "لدى", "بين"}

_WORD = re.compile(r"[^\W_]+", re.UNICODE)


def _words(text):
    t = unicodedata.normalize("NFKD", (text or "").lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return _WORD.findall(t)


def detect_language(title: str, text: str):
    sample = (title or "") + " " + (text or "")[:4000]
    letters = [c for c in sample if c.isalpha()]
    if not letters:
        return "other", 0.0
    ar_ratio = sum(1 for c in letters if "؀" <= c <= "ۿ") / len(letters)
    if ar_ratio > 0.25:
        return "ar", ar_ratio
    toks = _words(sample)
    if not toks:
        return "other", ar_ratio
    counts = {}
    for name, lex in (("fi", _FI), ("fr", _FR), ("en", _EN), ("es", _ES), ("de", _DE)):
        counts[name] = sum(1 for t in toks if t in lex)
    best = max(counts, key=counts.get)
    if counts[best] < 3:
        return ("ar" if ar_ratio > 0.05 else "other"), ar_ratio
    return best, ar_ratio


def spam_check(url: str, title: str, text: str):
    """Return (excluded: bool, reasons: list[str], lang: str)."""
    reasons = []
    host = host_key(url)
    raw_host = ""
    try:
        from urllib.parse import urlparse
        raw_host = (urlparse(url).hostname or "").lower()
    except Exception:
        pass
    lang, ar_ratio = detect_language(title, text)
    toks = _words((title or "") + " " + (text or "")[:6000])
    n = max(1, len(toks))
    casino_hits = sum(1 for t in toks if t in _CASINO)
    if casino_hits >= 3 and casino_hits / n >= 0.008:
        reasons.append(f"casino-density({casino_hits})")
    if lang not in ("ar", "fr", "en", "es"):
        reasons.append(f"foreign-language({lang})")
    sub = raw_host[: -len(host)] if raw_host.endswith(host) and raw_host != host else ""
    sub = sub.rstrip(".")
    if sub and sub not in ("www", "ar", "fr", "en", "app", "m") and lang not in ("ar", "fr", "en", "es"):
        reasons.append(f"subdomain-parasite({sub})")
    return (len(reasons) > 0), reasons, lang
