"""
Ingest Boericke's Pocket Manual of Homoeopathic Materia Medica (1927, 9th ed.)
from the Médi-T digitization at homeoint.org.

The underlying text is public domain in the US (W. Boericke d. 1929; 1927 publication).
We extract structured data into our schema rather than mirror the source HTML.

Output: data/processed/boericke.json — list of remedy records keyed by source_id boericke-1927.

Re-runnable: caches raw HTML in data/raw/boericke/<letter>/<slug>.htm
Polite: 0.5s delay between fetches.
"""
from __future__ import annotations

import json
import re
import ssl
import string
import sys
import time
import urllib.request
from html import unescape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "boericke"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

BASE = "http://www.homeoint.org/books/boericmm/"
SOURCE_ID = "boericke-1927"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

# --- Section label normalization ---------------------------------------------
# Boericke uses bold red labels like "Mind.--", "Head.--", "Stomach.--".
# Map them to our schema fields.
SECTION_MAP = {
    "mind": "mental_emotional",
    "head": "head",
    "eyes": "eyes",
    "ears": "ears",
    "nose": "nose",
    "face": "face",
    "mouth": "mouth_throat",
    "throat": "mouth_throat",
    "teeth": "mouth_throat",
    "tongue": "mouth_throat",
    "stomach": "stomach_abdomen",
    "abdomen": "stomach_abdomen",
    "rectum": "stool_rectum",
    "stool": "stool_rectum",
    "urine": "urinary",
    "urinary": "urinary",
    "kidneys": "urinary",
    "bladder": "urinary",
    "male": "male",
    "female": "female",
    "respiratory": "respiratory",
    "larynx": "respiratory",
    "cough": "respiratory",
    "chest": "chest_heart",
    "heart": "chest_heart",
    "back": "back",
    "neck": "back",
    "extremities": "extremities",
    "limbs": "extremities",
    "sleep": "sleep",
    "skin": "skin",
    "fever": "fever_chill",
    "chill": "fever_chill",
    "generalities": "generalities",
}


def fetch(url: str, cache_path: Path) -> str:
    """Fetch URL with on-disk cache. Returns decoded text."""
    if cache_path.exists():
        raw = cache_path.read_bytes()
    else:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, context=CTX, timeout=30) as r:
            raw = r.read()
        cache_path.write_bytes(raw)
        time.sleep(0.5)  # be polite
    # Médi-T pages are cp1252 / iso-8859-1
    for enc in ("cp1252", "iso-8859-1", "utf-8"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


# --- Index parsing -----------------------------------------------------------
# Index pages list per-letter remedies as <a href="<letter>/<slug>.htm">SLUG</a> ------> FULL NAME<br>
INDEX_LINE_RE = re.compile(
    r'<a href="([a-z])/([a-z0-9_\-]+)\.htm"[^>]*>([A-Z0-9\-_]+)</a>\s*-+&gt;\s*([^<]+)<br',
    re.IGNORECASE,
)


def parse_index(letter: str, html: str) -> list[dict]:
    out = []
    for m in INDEX_LINE_RE.finditer(html):
        ltr, slug, abbr, fullname = m.groups()
        if ltr.lower() != letter:
            continue
        out.append({
            "letter": ltr.lower(),
            "slug": slug.lower(),
            "abbreviation": abbr.strip(),
            "full_name": clean_text(fullname),
        })
    return out


# --- Remedy page parsing -----------------------------------------------------
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SECTION_HEADER_RE = re.compile(r"\b([A-Z][A-Za-z\- ]{2,30})\.\-\-")


def clean_text(s: str) -> str:
    s = unescape(s)
    s = TAG_RE.sub("", s)
    # Médi-T uses Windows-1252 chars and ligatures; map common cases
    s = (s
         .replace("\u0091", "'").replace("\u0092", "'")
         .replace("\u0093", '"').replace("\u0094", '"')
         .replace("\u0096", "-").replace("\u0097", "-"))
    s = WS_RE.sub(" ", s).strip()
    return s


def extract_title_and_common(html: str) -> tuple[str, str]:
    # Title block: <font size="5" color="#800000">NAME<br></font>Common name</b>
    m = re.search(
        r'<font size="5"[^>]*color="#800000"[^>]*>(.*?)</font>(.*?)</b>',
        html, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return "", ""
    title = clean_text(m.group(1))
    common = clean_text(m.group(2))
    return title, common


def split_sections(body_html: str) -> list[tuple[str, str]]:
    """
    Boericke section headers look like:
        <font color="#ff0000"><b><p align="justify">Head.--</b></font>...
    We strip tags and then split on the SECTION_HEADER_RE markers.
    """
    text = clean_text(body_html)
    # Find all section header positions
    matches = list(SECTION_HEADER_RE.finditer(text))
    if not matches:
        return [("intro", text)]

    sections: list[tuple[str, str]] = []
    # Anything before first match is the intro/lead paragraph
    intro = text[: matches[0].start()].strip()
    if intro:
        sections.append(("intro", intro))
    for i, m in enumerate(matches):
        label = m.group(1).strip().lower().split()[0]  # first word, e.g. "head"
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip(" .-")
        sections.append((label, body))
    return sections


def parse_modalities(text: str) -> dict:
    """Modalities text reads like 'Better in open air; worse in warm room, ...'."""
    out: dict[str, list[dict]] = {"better_from": [], "worse_from": []}
    # Split on Better / Worse markers
    parts = re.split(r"\b(Better|Worse)\b", text, flags=re.IGNORECASE)
    current = None
    for p in parts:
        pl = p.strip().lower()
        if pl == "better":
            current = "better_from"
        elif pl == "worse":
            current = "worse_from"
        elif current and p.strip():
            chunk = p.strip(" .,;:-")
            if chunk:
                out[current].append({"text": chunk, "source_id": SOURCE_ID})
    return out


def parse_dose(text: str) -> dict:
    return {
        "notes": text.strip(),
    }


def parse_relationships(text: str) -> dict:
    rel: dict[str, list[str]] = {}
    # Common patterns: "Complementary: X; Y." "Compare: X." "Antidote: X."
    patterns = {
        "complementary": r"complementary[^:]*:\s*([^.]+)",
        "antidoted_by": r"antidote(?:s)?(?:\s+to\s+it)?[^:]*:\s*([^.]+)",
        "follows_well": r"follows?\s+well[^:]*:\s*([^.]+)",
        "followed_well_by": r"followed\s+well\s+by[^:]*:\s*([^.]+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            items = [x.strip(" .,;") for x in re.split(r"[;,]", m.group(1)) if x.strip(" .,;")]
            if items:
                rel[key] = items
    return rel


def slugify(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def parse_remedy(letter: str, slug: str, abbr: str, full_name: str, html: str) -> dict:
    title, common = extract_title_and_common(html)
    if not title:
        title = full_name
    # Body region between the centered title block and the trailing copyright
    body_start = html.find('color="#800000">' + (title.split()[0] if title else ""))
    if body_start == -1:
        body_start = 0
    body_end = html.lower().find("copyright")
    if body_end == -1:
        body_end = len(html)
    body = html[body_start:body_end]

    sections = split_sections(body)

    record: dict = {
        "id": slugify(title or full_name),
        "names": {
            "primary": title or full_name,
            "latin": title or full_name,
            "common": [common] if common else [],
            "abbreviation": abbr,
        },
        "category": "homeopathic",
        "traditional": {},
        "provenance": {"sources": [SOURCE_ID]},
    }

    trad = record["traditional"]
    intro_text = None
    for label, body_text in sections:
        if label == "intro":
            intro_text = body_text
            continue
        if label == "modalities":
            trad["modalities"] = parse_modalities(body_text)
            continue
        if label == "dose":
            trad["dosing"] = parse_dose(body_text)
            continue
        if label == "relationship" or label == "relationships":
            rel = parse_relationships(body_text)
            if rel:
                trad["relationships"] = rel
            continue

        field = SECTION_MAP.get(label)
        if not field:
            # Unknown header — keep under generalities with the original label
            field = "generalities"
            entry_text = f"[{label.title()}] {body_text}"
        else:
            entry_text = body_text
        trad.setdefault(field, []).append({
            "text": entry_text,
            "source_id": SOURCE_ID,
        })

    if intro_text:
        trad.setdefault("keynotes", []).append({"text": intro_text, "source_id": SOURCE_ID})

    return record


# --- Driver ------------------------------------------------------------------

def main(letters: str | None = None) -> None:
    letters_to_do = letters.lower() if letters else string.ascii_lowercase
    all_remedies: list[dict] = []
    for letter in letters_to_do:
        idx_url = f"{BASE}{letter}.htm"
        idx_path = RAW_DIR / f"{letter}.htm"
        try:
            idx_html = fetch(idx_url, idx_path)
        except Exception as e:
            print(f"[!] index {letter}: {e}", file=sys.stderr)
            continue
        entries = parse_index(letter, idx_html)
        print(f"[{letter}] {len(entries)} remedies")
        for e in entries:
            url = f"{BASE}{e['letter']}/{e['slug']}.htm"
            cache = RAW_DIR / e["letter"] / f"{e['slug']}.htm"
            try:
                page = fetch(url, cache)
            except Exception as exc:
                print(f"  [!] {e['slug']}: {exc}", file=sys.stderr)
                continue
            try:
                rec = parse_remedy(e["letter"], e["slug"], e["abbreviation"], e["full_name"], page)
                all_remedies.append(rec)
            except Exception as exc:
                print(f"  [!] parse {e['slug']}: {exc}", file=sys.stderr)

    out_path = PROCESSED_DIR / "boericke.json"
    out_path.write_text(json.dumps(all_remedies, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(all_remedies)} remedies -> {out_path}")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg)
