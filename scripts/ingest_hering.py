"""
Ingest Hering's Guiding Symptoms of Our Materia Medica (1879-1891, 10 vols)
from homeoint.org Médi-T digitization. Public domain in US (Hering d. 1880).

Hering's structure on Médi-T:
  - Per-letter index at /hering/<letter>.htm with links to <letter>/<slug>.htm
  - Each remedy's main page <slug>.htm is just a TOC pointing to 48 numbered
    subsection pages (<slug>-1.htm ... <slug>-10.htm)
  - There is also a consolidated "Full text" file <slug>-kn3.htm with all 48
    sections in one document, each preceded by <a name="N"></a> for N=1..48.

We use the consolidated kn3 file. Section numbering is fixed across remedies:

  1.Mind  2.Sensorium  3.Inner head  4.Outer head  5.Sight/eyes
  6.Hearing/ears  7.Smell/nose  8.Upper face  9.Lower face  10.Teeth/gums
  11.Taste/speech/tongue  12.Inner mouth  13.Palate/throat
  14.Appetite/thirst/desires  15.Eating/drinking  16.Hiccough/belching/nausea
  17.Scrobiculum/stomach  18.Hypochondria  19.Abdomen/loins  20.Stools/rectum
  21.Urinary organs  22.Male sexual  23.Female sexual  24.Pregnancy/parturition
  25.Voice/larynx/trachea  26.Respiration  27.Cough  28.Inner chest/lungs
  29.Heart/pulse/circulation  30.Outer chest  31.Neck/back  32.Upper limbs
  33.Lower limbs  34.Limbs in general  35.Rest/position/motion  36.Nerves
  37.Sleep  38.Time  39.Temperature/weather  40.Fever
  41.Attacks/periodicity  42.Locality/direction  43.Sensations  44.Tissues
  45.Touch/injuries  46.Skin  47.Stage of life/constitution  48.Relations

Within a section, individual symptoms are separated by <br> tags.
"""
from __future__ import annotations

import json
import re
import ssl
import string
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from html import unescape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "hering"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE = "http://www.homeoint.org/hering/"
SOURCE_ID = "hering-guiding-1879"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
ANCHOR_NUM_RE = re.compile(r'<a\s+name="(\d{1,2})"', re.IGNORECASE)

# Letter index points to remedy main pages: <letter>/<slug>.htm
# Exclude things like <slug>-1.htm, <slug>-kn1.htm. Slug must not contain '-' followed
# by digits or 'kn'.
INDEX_RE = re.compile(
    r'<a href="([a-z])/([a-z0-9_\-]+)\.htm"[^>]*>([^<]+)</a>',
    re.IGNORECASE,
)

# section number -> schema field
HERING_SECTION_MAP: dict[int, str] = {
    1: "mental_emotional",
    2: "mental_emotional",
    3: "head",
    4: "head",
    5: "eyes",
    6: "ears",
    7: "nose",
    8: "face",
    9: "face",
    10: "mouth_throat",
    11: "mouth_throat",
    12: "mouth_throat",
    13: "mouth_throat",
    14: "stomach_abdomen",
    15: "stomach_abdomen",
    16: "stomach_abdomen",
    17: "stomach_abdomen",
    18: "stomach_abdomen",
    19: "stomach_abdomen",
    20: "stool_rectum",
    21: "urinary",
    22: "male",
    23: "female",
    24: "female",
    25: "respiratory",
    26: "respiratory",
    27: "respiratory",
    28: "chest_heart",
    29: "chest_heart",
    30: "chest_heart",
    31: "back",
    32: "extremities",
    33: "extremities",
    34: "extremities",
    35: "modalities",
    36: "generalities",
    37: "sleep",
    38: "modalities",
    39: "modalities",
    40: "fever_chill",
    41: "generalities",
    42: "generalities",
    43: "generalities",
    44: "generalities",
    45: "generalities",
    46: "skin",
    47: "generalities",
    48: "relationships",
}


def fetch(url: str, cache_path: Path, allow_404: bool = False) -> str | None:
    if cache_path.exists():
        raw = cache_path.read_bytes()
    else:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, context=CTX, timeout=30) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404 and allow_404:
                # Cache an empty marker so we don't refetch
                cache_path.write_bytes(b"")
                return None
            raise
        cache_path.write_bytes(raw)
        time.sleep(0.4)
    if not raw:
        return None
    for enc in ("cp1252", "iso-8859-1", "utf-8"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def clean_text(s: str) -> str:
    s = unescape(s)
    s = TAG_RE.sub("", s)
    s = WS_RE.sub(" ", s).strip()
    return s


def slugify(name: str) -> str:
    # Normalize so Æ→AE, é→e, etc.; expand the ligature ourselves first
    s = name.replace("\u00c6", "AE").replace("\u00e6", "ae")
    s = s.replace("\u0152", "OE").replace("\u0153", "oe")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def split_section_lines(section_html: str) -> list[str]:
    """Convert <br> to newlines, strip tags, return non-empty lines.
    Also drops the leading 'SECTION NAME. [N] [link]' header line if present."""
    text = BR_RE.sub("\n", section_html)
    # Replace closing </p> with newline too so paragraphs split
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = unescape(TAG_RE.sub("", text))
    lines = []
    for raw in text.split("\n"):
        ln = WS_RE.sub(" ", raw).strip(" \xa0\t.,;")
        if not ln:
            continue
        # Skip the section header e.g. "MIND. [1] [Acon.]"
        if re.match(r"^[A-Z][A-Z ,/&\-]{1,40}\.\s*\[\d{1,2}\]", ln):
            continue
        if len(ln) < 3:
            continue
        lines.append(ln)
    return lines


def parse_remedy(slug: str, html: str, fallback_name: str) -> dict | None:
    # Title from <title> tag: "Aconitum Napellus. - THE GUIDING SYMPTOMS..."
    m = re.search(r"<title>([^<]+)</title>", html, re.IGNORECASE)
    if m:
        raw_title = unescape(m.group(1))
        title = raw_title.split(" - ")[0].strip().rstrip(".").strip()
    else:
        title = fallback_name.rstrip(".").strip()

    anchors = list(ANCHOR_NUM_RE.finditer(html))
    # Keep numbered anchors only (already filtered by regex)
    if not anchors:
        return None

    sections_by_field: dict[str, list[str]] = {}
    for i, m in enumerate(anchors):
        n = int(m.group(1))
        if n < 1 or n > 48:
            continue
        start = m.end()
        end = anchors[i + 1].start() if i + 1 < len(anchors) else len(html)
        section_html = html[start:end]
        lines = split_section_lines(section_html)
        if not lines:
            continue
        field = HERING_SECTION_MAP.get(n, "generalities")
        sections_by_field.setdefault(field, []).extend(lines)

    if not sections_by_field:
        return None

    record: dict = {
        "id": slugify(title),
        "names": {
            "primary": title,
            "latin": title,
        },
        "category": "homeopathic",
        "traditional": {},
        "provenance": {"sources": [SOURCE_ID]},
    }
    trad = record["traditional"]
    for field, lines in sections_by_field.items():
        if field == "relationships":
            trad["relationships"] = {"_raw": lines}
            continue
        # Merge into a single keynote-style entry per field
        merged = ". ".join(lines)
        if not merged.endswith("."):
            merged += "."
        trad.setdefault(field, []).append({"text": merged, "source_id": SOURCE_ID})

    return record


def collect_remedies_for_letter(letter: str, idx_html: str) -> list[tuple[str, str, str]]:
    out = []
    seen = set()
    for m in INDEX_RE.finditer(idx_html):
        ltr, slug, name = m.groups()
        if ltr.lower() != letter:
            continue
        slug_l = slug.lower()
        # Exclude sub-pages: <slug>-1, <slug>-kn1, etc.
        if re.search(r"-(?:kn)?\d+$", slug_l):
            continue
        if slug_l in seen:
            continue
        seen.add(slug_l)
        out.append((ltr.lower(), slug_l, clean_text(name)))
    return out


def main(letters: str | None = None) -> None:
    letters_to_do = letters.lower() if letters else string.ascii_lowercase
    all_records: list[dict] = []
    missing_kn3 = 0

    for letter in letters_to_do:
        idx_url = f"{BASE}{letter}.htm"
        idx_cache = RAW_DIR / f"{letter}.htm"
        try:
            idx_html = fetch(idx_url, idx_cache)
        except Exception as e:
            print(f"[!] index {letter}: {e}", file=sys.stderr)
            continue
        if not idx_html:
            continue
        entries = collect_remedies_for_letter(letter, idx_html)
        print(f"[{letter}] {len(entries)} remedies")

        for ltr, slug, name in entries:
            # Try consolidated full-text file first; fall back to main slug page
            # which, for shorter remedies, contains the numbered anchors directly.
            page = None
            for fname in (f"{slug}-kn3.htm", f"{slug}.htm"):
                url = f"{BASE}{ltr}/{fname}"
                cache = RAW_DIR / ltr / fname
                try:
                    page = fetch(url, cache, allow_404=True)
                except Exception as exc:
                    print(f"  [!] {fname}: {exc}", file=sys.stderr)
                    page = None
                if page:
                    break
            if not page:
                missing_kn3 += 1
                continue
            try:
                rec = parse_remedy(slug, page, name)
                if rec:
                    all_records.append(rec)
            except Exception as exc:
                print(f"  [!] parse {slug}: {exc}", file=sys.stderr)

    out = PROCESSED_DIR / "hering.json"
    out.write_text(json.dumps(all_records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(all_records)} records -> {out}")
    if missing_kn3:
        print(f"({missing_kn3} remedies had no kn3 consolidated file)")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg)
