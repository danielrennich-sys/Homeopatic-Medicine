"""
Ingest Allen's Encyclopedia of Pure Materia Medica (Timothy F. Allen,
1874-1879, 12 vols) from the Médi-T digitization on homeoint.org.

Structure:
  /allen/index.php  -> letter index (a.htm..z.htm)
  /allen/<letter>.htm  -> list of remedies (links to <letter>/<slug>.htm)
  /allen/<letter>/<slug>.htm  -> TOC page (intro + links to sub-pages)
  /allen/<letter>/<slug>-N.htm  -> body sub-pages with anchored sections
                                   (mind, head, eyes, ..., generalities)

Each numbered symptom is in the form `<number>. <text>` (sometimes with
authority refs in superscript). We grab the full prose per anatomical
section, plus a clean Authorities/intro paragraph.
"""
from __future__ import annotations

import json
import re
import ssl
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from html import unescape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "allen"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE = "http://www.homeoint.org/allen/"
SOURCE_ID = "allen-encyclopedia-1874"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SCRIPT_RE = re.compile(r"<script.*?</script>", re.DOTALL | re.IGNORECASE)
STYLE_RE = re.compile(r"<style.*?</style>", re.DOTALL | re.IGNORECASE)

# Anchored sections within sub-pages — keep keys lowercase
SECTION_ANCHORS = [
    "mind", "head", "scalp", "eyes", "ears", "nose", "face", "mouth",
    "teeth", "tongue", "throat", "stomach", "abdomen", "stool", "rectum",
    "anus", "urinary", "kidneys", "bladder", "urethra", "urine",
    "genitalia", "male", "female", "pregnancy", "respiratory", "larynx",
    "trachea", "voice", "cough", "expectoration", "chest", "lungs",
    "heart", "pulse", "back", "neck", "extremities", "supextrem",
    "infextrem", "limbs", "arms", "legs", "generalities", "skin",
    "sleep", "fever", "supplement", "modalities",
]


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
                cache_path.write_bytes(b"")
                return None
            raise
        cache_path.write_bytes(raw)
        time.sleep(0.3)
    if not raw:
        return None
    return raw.decode("iso-8859-1", errors="replace")


def slugify(name: str) -> str:
    s = name.replace("\u00c6", "AE").replace("\u00e6", "ae")
    s = s.replace("\u0152", "OE").replace("\u0153", "oe")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def clean_text(s: str) -> str:
    s = SCRIPT_RE.sub(" ", s)
    s = STYLE_RE.sub(" ", s)
    s = unescape(TAG_RE.sub(" ", s))
    s = WS_RE.sub(" ", s).strip()
    return s


def parse_letter_index(html: str, letter: str) -> list[str]:
    """Return remedy base slugs (e.g. 'acon', 'acet-ac') for one letter."""
    pat = re.compile(rf'href="({letter}/[a-z0-9\-]+)\.htm"', re.IGNORECASE)
    seen = set()
    out = []
    for m in pat.finditer(html):
        slug = m.group(1).split("/", 1)[1]
        if slug in seen:
            continue
        seen.add(slug)
        out.append(slug)
    return out


def parse_toc_page(html: str, slug: str) -> tuple[str, list[str]]:
    """Return (display_title, [sub-page filenames])."""
    title_m = re.search(
        r"<p[^>]*align=\"CENTER\"[^>]*>([A-Z][^<]+?)</p>",
        html,
    )
    title = clean_text(title_m.group(1)) if title_m else slug.upper()
    title = title.rstrip(". ")

    pat = re.compile(rf'href="({re.escape(slug)}-\d+\.htm)"', re.IGNORECASE)
    seen = set()
    out = []
    for m in pat.finditer(html):
        f = m.group(1)
        if f in seen:
            continue
        seen.add(f)
        out.append(f)
    return title, out


def extract_intro(toc_html: str) -> str:
    """The TOC page sometimes has natural-order/preparation/common-name info."""
    m = re.search(
        r"</p>\s*<b><font[^>]*>\s*<p[^>]*>([^<]+?)</p>",
        toc_html,
    )
    if m:
        return clean_text(m.group(1))
    return ""


HEADER_RE = re.compile(
    r'<a name="([a-z][a-zA-Z0-9_]*)"[^>]*>',
    re.IGNORECASE,
)


def extract_sections_from_subpage(html: str) -> dict[str, str]:
    """Slice the body into sections by `<a name="...">` markers that match
    one of SECTION_ANCHORS. Returns {anchor_name: cleaned_text}."""
    # Restrict to body
    body_m = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL | re.IGNORECASE)
    body = body_m.group(1) if body_m else html

    anchor_positions = []
    for m in HEADER_RE.finditer(body):
        name = m.group(1).lower()
        if name in SECTION_ANCHORS or name.startswith("s") and name[1:].isdigit():
            anchor_positions.append((m.start(), m.end(), name))

    sections: dict[str, str] = {}
    if not anchor_positions:
        # Some short remedy pages have no anchors at all — return whole body
        text = clean_text(body)
        if len(text) > 200:
            sections["body"] = text
        return sections

    # Build slices: from each named-section anchor to the next named-section anchor
    named = [a for a in anchor_positions if a[2] in SECTION_ANCHORS]
    if not named:
        return sections
    for i, (_, hend, name) in enumerate(named):
        next_start = named[i + 1][0] if i + 1 < len(named) else len(body)
        chunk = body[hend:next_start]
        text = clean_text(chunk)
        if not text or len(text) < 20:
            continue
        # Aggregate if section name repeats across pages
        if name in sections:
            sections[name] += " " + text
        else:
            sections[name] = text
    return sections


# Map Allen anchor names to schema fields under traditional
ANCHOR_TO_FIELD = {
    "mind": "mentals",
    "head": "head",
    "scalp": "head",
    "eyes": "eyes",
    "ears": "ears",
    "nose": "nose",
    "face": "face",
    "mouth": "mouth",
    "teeth": "teeth",
    "tongue": "mouth",
    "throat": "throat",
    "stomach": "stomach",
    "abdomen": "abdomen",
    "stool": "rectum",
    "rectum": "rectum",
    "anus": "rectum",
    "urinary": "urinary",
    "kidneys": "urinary",
    "bladder": "urinary",
    "urethra": "urinary",
    "urine": "urinary",
    "genitalia": "genitalia",
    "male": "male",
    "female": "female",
    "pregnancy": "female",
    "respiratory": "respiratory",
    "larynx": "respiratory",
    "trachea": "respiratory",
    "voice": "respiratory",
    "cough": "cough",
    "expectoration": "cough",
    "chest": "chest",
    "lungs": "chest",
    "heart": "heart",
    "pulse": "heart",
    "back": "back",
    "neck": "back",
    "extremities": "extremities",
    "supextrem": "upper_extremities",
    "infextrem": "lower_extremities",
    "limbs": "extremities",
    "arms": "upper_extremities",
    "legs": "lower_extremities",
    "generalities": "generalities",
    "skin": "skin",
    "sleep": "sleep",
    "fever": "fever",
    "supplement": "generalities",
    "modalities": "modalities",
}


def main() -> None:
    letters = "abcdefghijklmnopqrstuvwxyz"
    all_slugs: list[tuple[str, str]] = []  # (letter, slug)
    for letter in letters:
        cache = RAW_DIR / f"_letter_{letter}.html"
        try:
            page = fetch(BASE + f"{letter}.htm", cache, allow_404=True)
        except Exception as e:
            print(f"  [letter {letter} !] {e}", file=sys.stderr)
            continue
        if not page:
            continue
        for slug in parse_letter_index(page, letter):
            all_slugs.append((letter, slug))
    print(f"Allen index: {len(all_slugs)} remedy slugs")

    records = []
    for i, (letter, slug) in enumerate(all_slugs):
        toc_url = BASE + f"{letter}/{slug}.htm"
        toc_cache = RAW_DIR / letter / f"{slug}.htm"
        try:
            toc_html = fetch(toc_url, toc_cache, allow_404=True)
        except Exception as e:
            print(f"  [{slug} !] {e}", file=sys.stderr)
            continue
        if not toc_html:
            continue
        title, subpages = parse_toc_page(toc_html, slug)
        intro = extract_intro(toc_html)

        all_sections: dict[str, str] = {}
        # If the remedy is short there may be no sub-pages — body lives on TOC page
        pages_to_parse = subpages if subpages else [f"{slug}.htm"]
        for sub in pages_to_parse:
            sub_url = BASE + f"{letter}/{sub}"
            sub_cache = RAW_DIR / letter / sub
            try:
                sub_html = fetch(sub_url, sub_cache, allow_404=True)
            except Exception as e:
                print(f"  [{sub} !] {e}", file=sys.stderr)
                continue
            if not sub_html:
                continue
            for k, v in extract_sections_from_subpage(sub_html).items():
                if k in all_sections:
                    all_sections[k] += " " + v
                else:
                    all_sections[k] = v

        if not all_sections and not intro:
            continue
        # Build record
        primary = title.title() if title.isupper() else title
        record = {
            "id": slugify(primary),
            "names": {
                "primary": primary,
                "latin": primary,
            },
            "category": "homeopathic",
            "traditional": {},
            "provenance": {"sources": [SOURCE_ID]},
        }
        trad = record["traditional"]
        if intro and len(intro) > 20:
            trad["keynotes"] = [{"text": intro[:4000], "source_id": SOURCE_ID}]
        # Group sections to schema fields
        grouped: dict[str, list[str]] = {}
        for anchor, text in all_sections.items():
            field = ANCHOR_TO_FIELD.get(anchor, "generalities")
            grouped.setdefault(field, []).append(text)
        for field, chunks in grouped.items():
            joined = "\n".join(chunks)[:30000]
            trad.setdefault(field, []).append({
                "text": joined, "source_id": SOURCE_ID,
            })

        # Drop records with insufficient content
        body_len = sum(len(v) for v in all_sections.values())
        if body_len < 300 and not intro:
            continue
        records.append(record)
        if (i + 1) % 50 == 0:
            print(f"  ... {i + 1}/{len(all_slugs)}")

    out = PROCESSED_DIR / "allen_encyclopedia.json"
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(records)} records -> {out}")


if __name__ == "__main__":
    main()
