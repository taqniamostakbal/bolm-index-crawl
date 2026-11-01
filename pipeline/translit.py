# Copied verbatim (logic unchanged) from the proven stage-2 pipeline:
#   ~/workspace/bolm-own-index/code/translit.py
# Productized into the Phase-3 crawl repo (taqniamostakbal/bolm-index-crawl) on 2026-11-01.
"""Query-side Latin-Darija transliteration / variant expansion (v2).

Finding from the stage-2 proof: Latin-script Darija is nearly ABSENT from
the .ma web itself (kifach/atay = 0 occurrences in 1,814 docs), so recall
for Latin queries must come from the query side: each Latin token expands
to its Arabic-script equivalent(s), merged with the literal terms.

Expansion model: a query becomes GROUPS. Each original token is one group
{literal, variant1, variant2, ...}; coverage counts a group once if ANY
variant matches. Dictionary first (curated Darija vocabulary incl. the
digit-letters 3/7/9 spellings); a conservative rule-based fallback maps
unknown Latin tokens to one approximate Arabic form.
"""
import re

# latin token (normalized: lowercase) -> arabic variants (unnormalized;
# normalize() is applied by the caller pipeline)
_DICT = {
    # question words / function words
    "kifach": ["كيفاش"], "kifash": ["كيفاش"], "kif": ["كيف"],
    "chnou": ["شنو"], "chnu": ["شنو"], "shnou": ["شنو"], "ashnou": ["أشنو", "شنو"],
    "3lach": ["علاش"], "alach": ["علاش"], "3lash": ["علاش"], "lach": ["علاش"],
    "fin": ["فين"], "fayn": ["فين"], "mnin": ["منين"],
    "ch7al": ["شحال"], "chhal": ["شحال"], "shhal": ["شحال"],
    "bch7al": ["بشحال", "شحال"], "bchhal": ["بشحال", "شحال"],
    "imta": ["امتى", "متى"], "emta": ["امتى", "متى"],
    "chkon": ["شكون"], "chkoun": ["شكون"], "skon": ["شكون"],
    "wach": ["واش"], "wash": ["واش"],
    "kayn": ["كاين"], "kayen": ["كاين"], "kayna": ["كاينة"],
    "bghit": ["بغيت"], "bghina": ["بغينا"],
    "wa9t": ["وقت", "أوقات"], "waqt": ["وقت", "أوقات"],
    " daba": ["دابا"], "daba": ["دابا"],
    # content words (battery + common intents)
    "atay": ["أتاي", "الشاي", "شاي"], "tay": ["أتاي", "شاي"],
    "nsayeb": ["نصايب", "نصيب"], "nsayb": ["نصايب"], "sayeb": ["صايب"],
    "mazout": ["المازوط", "مازوط", "الغازوال"], "lmazout": ["المازوط", "مازوط", "الغازوال"],
    "taman": ["ثمن", "تمن", "سعر"], "thaman": ["ثمن", "تمن"], "teman": ["تمن", "ثمن"],
    "salat": ["صلاة", "الصلاة"], "sala": ["صلاة"], "salate": ["صلاة"],
    "souk": ["سوق", "السوق"], "so9": ["سوق"],
    "casa": ["كازا", "الدار البيضاء"], "casablanca": ["الدار البيضاء", "كازا"],
    "dar": ["دار"], "bida": ["البيضاء"],
    "ndir": ["ندير", "ندير"], "dir": ["دير"],
    "hesab": ["حساب"], "hissab": ["حساب"],
    "banka": ["بنك"], "bank": ["بنك"], "banque": ["بنك"],
    "flous": ["فلوس"], "flos": ["فلوس"],
    "khobz": ["خبز"], "zit": ["زيت"],
    "train": ["قطار", "القطار"], "ticket": ["تذكرة"], "billet": ["تذكرة"],
    "pharmacie": ["صيدلية"], "saydalia": ["صيدلية"],
    "garde": ["حراسة", "الحراسة"],
    "meteo": ["الطقس", "طقس"], "ta9s": ["طقس"],
    "gheda": ["غدا"], "ghda": ["غدا"], "demain": ["غدا"],
    "rabat": ["الرباط"], "agadir": ["أكادير"], "marrakech": ["مراكش"],
    "maroc": ["المغرب"], "maghreb": ["المغرب"],
    "priere": ["صلاة", "الصلاة"], "prayer": ["صلاة"],
    "horaire": ["أوقات", "مواقيت"], "horaires": ["أوقات", "مواقيت"],
    "prix": ["ثمن", "سعر"], "taux": ["سعر"], "change": ["صرف"],
    "dirham": ["درهم", "الدرهم"], "euro": ["يورو", "اليورو"],
    "resultat": ["نتائج"], "bac": ["بكالوريا"],
    "universite": ["جامعة"], "jamia": ["جامعة"],
}

_DIGIT = {"7": "ح", "9": "ق", "3": "ع", "5": "خ", "2": "ء"}
_MULTI = [("ch", "ش"), ("kh", "خ"), ("gh", "غ"), ("th", "ث"), ("dh", "ذ"),
          ("sh", "ش"), ("ou", "و"), ("aa", "ا"), ("ee", "ي"), ("ii", "ي")]


def _rule_fallback(tok: str):
    """One approximate Arabic form for an unknown Latin token, or None."""
    if not re.fullmatch(r"[a-z0-9]+", tok) or len(tok) < 3:
        return None
    s = tok
    for a, b in _MULTI:
        s = s.replace(a, b)
    out = []
    for ch in s:
        if ch in _DIGIT:
            out.append(_DIGIT[ch])
        elif ch.isalpha() and ch.isascii():
            out.append({"a": "ا", "b": "ب", "d": "د", "f": "ف", "g": "غ",
                        "h": "ه", "i": "ي", "j": "ج", "k": "ك", "l": "ل",
                        "m": "م", "n": "ن", "o": "و", "p": "ب", "q": "ق",
                        "r": "ر", "s": "س", "t": "ت", "u": "و", "v": "ف",
                        "w": "و", "y": "ي", "z": "ز", "e": "ي", "c": "ك",
                        "x": "كس"}.get(ch, ""))
        else:
            out.append(ch)
    form = "".join(out)
    return form if len(form) >= 3 else None


def expand_groups(norm_query: str, normalize_fn):
    """norm_query -> list of groups; each group is a set of normalized
    term variants (including the literal token). Latin tokens get their
    Arabic variants merged in; Arabic tokens stay singleton groups."""
    groups = []
    for tok in norm_query.split():
        variants = {tok}
        if re.search(r"[a-z]", tok):
            for v in _DICT.get(tok, []):
                for piece in normalize_fn(v).split():
                    variants.add(piece)
            fb = _rule_fallback(tok)
            if fb:
                for piece in normalize_fn(fb).split():
                    if len(piece) >= 3:
                        variants.add(piece)
        groups.append(variants)
    return groups
