"""
Ingest Mrs. Grieve's A Modern Herbal (1931) from botanical.com.

Source: https://www.botanical.com/botanical/mgmh/mgmh.html
850+ herb entries organized alphabetically.

Each entry has structured info: Botanical name, Family, Synonyms,
Habitat, Description, Medicinal Action, Dosage, etc.

Public domain (1931, UK author d.1941 — copyright status varies by
jurisdiction but the text is widely reproduced and freely available).
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
RAW_DIR = ROOT / "data" / "raw" / "grieve"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://www.botanical.com/botanical/mgmh/"
SOURCE_ID = "grieve-modern-herbal-1931"

CTX = ssl.create_default_context()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml",
}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SCRIPT_RE = re.compile(r"<script.*?</script>", re.DOTALL | re.IGNORECASE)
STYLE_RE = re.compile(r"<style.*?</style>", re.DOTALL | re.IGNORECASE)

LETTERS = "abcdefghijklmnopqrstuvwxyz"


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
    return raw.decode("utf-8", errors="replace")


def slugify(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
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


def extract_title(html: str) -> str:
    m = re.search(r"<title>(.*?)</title>", html, re.DOTALL | re.IGNORECASE)
    if not m:
        return ""
    t = clean_text(m.group(1))
    # "A Modern Herbal | Abscess Root" -> "Abscess Root"
    if "|" in t:
        t = t.split("|", 1)[1].strip()
    return t


def extract_botanical(text: str) -> str:
    """Pull the 'Botanical: ...' field (Latin name)."""
    # Match "Botanical: <Latin name>" up to "Family:" or "---" or end of phrase
    m = re.search(r"Botanical:\s*(.+?)(?:\s+Family:|\s+---|\s+Description)", text)
    if m:
        name = m.group(1).strip().rstrip(".")
        # Sanity check — a real Latin name should be < 80 chars
        if len(name) < 80:
            return name
    return ""


def extract_herb_links(html: str, letter: str) -> list[str]:
    """Find all herb entry relative paths from a letter index page."""
    pat = re.compile(rf'\"({letter}/[a-z0-9]+\.html)\"', re.IGNORECASE)
    seen = set()
    out = []
    for m in pat.finditer(html):
        path = m.group(1)
        if path in seen:
            continue
        seen.add(path)
        out.append(path)
    return out


def parse_entry(html: str) -> dict | None:
    title = extract_title(html)
    if not title:
        return None

    # Extract text body — strip boilerplate before and after content
    body_text = clean_text(html)
    # Cut at the nav footer
    footer_m = re.search(r"A - B - C - D - E", body_text)
    if footer_m:
        body_text = body_text[:footer_m.start()].strip()
    # Cut Google Analytics boilerplate at start
    ga_end = re.search(r"ga\('send', 'pageview'\);", body_text)
    if ga_end:
        body_text = body_text[ga_end.end():].strip()
    if len(body_text) < 200:
        return None

    botanical = extract_botanical(body_text)
    primary = botanical if botanical else title

    # Extract key sections using ---Header--- pattern used by Grieve
    sections: dict[str, str] = {}
    section_pat = re.compile(r"---([A-Za-z ]+)---\s*(.*?)(?=---[A-Za-z ]+---|$)", re.DOTALL)
    for m in section_pat.finditer(body_text):
        hdr = m.group(1).strip()
        text = m.group(2).strip()
        if hdr and text and len(text) > 20:
            sections[hdr] = text

    if not sections:
        # Fallback: whole body
        sections["Description"] = body_text[:15000]

    record = {
        "id": slugify(primary),
        "names": {
            "primary": primary,
            "latin": botanical or primary,
            "common": [title] if title != botanical else [],
        },
        "category": "herbal",
        "traditional": {},
        "provenance": {"sources": [SOURCE_ID]},
    }
    trad = record["traditional"]

    for hdr, text in sections.items():
        hl = hdr.lower()
        if "synonym" in hl:
            syns = [s.strip() for s in text.split(",") if s.strip()]
            record["names"].setdefault("synonyms", []).extend(syns[:20])
        elif "habitat" in hl:
            trad.setdefault("habitat", []).append({
                "text": text[:3000], "source_id": SOURCE_ID,
            })
        elif "description" in hl:
            trad.setdefault("keynotes", []).append({
                "text": text[:6000], "source_id": SOURCE_ID,
            })
        elif "medicinal" in hl or "uses" in hl or "action" in hl:
            trad.setdefault("indications", []).append({
                "text": text[:8000], "source_id": SOURCE_ID,
            })
        elif "dosage" in hl or "dose" in hl:
            trad.setdefault("dosing", []).append({
                "text": text[:4000], "source_id": SOURCE_ID,
            })
        elif "constituents" in hl or "chemical" in hl:
            trad.setdefault("constituents", []).append({
                "text": text[:4000], "source_id": SOURCE_ID,
            })
        elif "cultivation" in hl or "preparation" in hl or "part used" in hl:
            trad.setdefault("preparation", []).append({
                "text": text[:4000], "source_id": SOURCE_ID,
            })
        elif "caution" in hl or "danger" in hl or "poison" in hl:
            record.setdefault("evidence", {}).setdefault(
                "documented_adverse_effects", []
            ).append({"text": text[:4000], "source": "Grieve"})
        else:
            trad.setdefault("generalities", []).append({
                "text": text[:4000], "source_id": SOURCE_ID,
            })

    if not trad:
        return None
    return record


def main() -> None:
    all_herb_paths: list[tuple[str, str]] = []  # (letter, relative_path)
    for letter in LETTERS:
        idx_url = BASE + f"comindx{letter}.html"
        cache = RAW_DIR / f"_index_{letter}.html"
        try:
            page = fetch(idx_url, cache, allow_404=True)
        except Exception as e:
            print(f"  [letter {letter} !] {e}", file=sys.stderr)
            continue
        if not page:
            continue
        paths = extract_herb_links(page, letter)
        for p in paths:
            all_herb_paths.append((letter, p))
    print(f"Grieve index: {len(all_herb_paths)} herb entries")

    records = []
    for i, (letter, rel_path) in enumerate(all_herb_paths):
        url = BASE + rel_path
        safe_name = rel_path.replace("/", "_")
        cache = RAW_DIR / safe_name
        try:
            page = fetch(url, cache, allow_404=True)
        except Exception as exc:
            print(f"  [!] {rel_path}: {exc}", file=sys.stderr)
            continue
        if not page:
            continue
        rec = parse_entry(page)
        if rec:
            records.append(rec)
        if (i + 1) % 100 == 0:
            print(f"  ... {i + 1}/{len(all_herb_paths)}")

    out = PROCESSED_DIR / "grieve_modern_herbal.json"
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(records)} records -> {out}")


if __name__ == "__main__":
    main()
