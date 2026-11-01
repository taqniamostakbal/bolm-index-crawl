# Copied verbatim (logic unchanged) from the proven stage-2 pipeline:
#   ~/workspace/bolm-own-index/code/normalize.py
# Productized into the Phase-3 crawl repo (taqniamostakbal/bolm-index-crawl) on 2026-11-01.
"""Arabic/French/Darija text normalization for the Bolm own-index proof.

Same function is applied to documents at index time and to queries at
search time, so foldings always match on both sides.

Rules:
- Unicode NFKD, then drop all combining marks (category Mn) -> strips Arabic
  harakat/diacritics and French accents (e -> e) in one pass.
- Tatweel (U+0640) removed.
- Arabic letter folding: إ أ آ ٱ -> ا ; ة -> ه ; ى -> ي ; ئ -> ي ; ؤ -> و ; ء dropped.
- Lowercase Latin. Digits kept (Latin-Darija uses 3/7/9 as letters: ch7al, wa9t).
- Anything that is not a letter/digit becomes a space; whitespace collapsed.
"""
import re
import unicodedata

_FOLD = str.maketrans({
    "إ": "ا", "أ": "ا", "آ": "ا", "ٱ": "ا",
    "ة": "ه", "ى": "ي", "ئ": "ي", "ؤ": "و", "ء": "",
    "ـ": "",  # tatweel
})

def normalize(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.translate(_FOLD)
    text = text.lower()
    text = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in text)
    return re.sub(r"\s+", " ", text).strip()

def tokens(text: str):
    return normalize(text).split()
