"""
Ingest NCCIH "Herbs at a Glance" evidence summaries.

Source: https://www.nccih.nih.gov/health/herbsataglance
Each entry has clean <h2> sections:
  - Background
  - How Much Do We Know?
  - What Have We Learned?
  - What Do We Know About Safety?
plus a "Common Names:" / "Latin Names:" intro block.

Output goes into the `evidence` block of each remedy record (not `traditional`),
since this is the modern systematic-evidence side of the parallel-table design.
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
RAW_DIR = ROOT / "data" / "raw" / "nccih"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://www.nccih.nih.gov/health/"
INDEX_URL = BASE + "herbsataglance"
SOURCE_ID = "nccih"

CTX = ssl.create_default_context()
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SCRIPT_RE = re.compile(r"<script.*?</script>", re.DOTALL | re.IGNORECASE)
STYLE_RE = re.compile(r"<style.*?</style>", re.DOTALL | re.IGNORECASE)
H2_SECTION_RE = re.compile(r"<h2[^>]*>(.*?)</h2>(.*?)(?=<h2[^>]*>|</main>)", re.DOTALL | re.IGNORECASE)

# Slugs that appear in the index but aren't herb entries
NON_HERB_SLUGS = {
    "atoz", "tips", "pain", "espanol", "herbsataglance", "stress",
    "providers", "covid-19", "consumers", "health-topics-a-z",
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
                cache_path.write_bytes(b"")
                return None
            raise
        cache_path.write_bytes(raw)
        time.sleep(0.5)
    if not raw:
        return None
    return raw.decode("utf-8", errors="replace")


def slugify(name: str) -> str:
    s = name.replace("\u00c6", "AE").replace("\u00e6", "ae")
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


def extract_meta(html: str) -> tuple[str, list[str], list[str]]:
    """Return (display_name, common_names, latin_names) from the intro block."""
    # Title from H1
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
    name = clean_text(h1.group(1)) if h1 else ""

    # The page text contains lines like "Common Names: foo, bar"
    # and "Latin Names: Echinacea purpurea, Echinacea angustifolia"
    text = clean_text(html[:50000])  # only intro region
    common, latin = [], []
    cm = re.search(r"Common Names?:\s*([^.]+?)(?:Latin Names?:|Background)", text, re.IGNORECASE)
    if cm:
        common = [c.strip(" .,;") for c in cm.group(1).split(",") if c.strip(" .,;")]
    lm = re.search(r"Latin Names?:\s*([^.]+?)(?:\.|Background)", text, re.IGNORECASE)
    if lm:
        latin = [l.strip(" .,;") for l in lm.group(1).split(",") if l.strip(" .,;")]
    return name, common, latin


def extract_sections(html: str) -> dict[str, str]:
    """Map H2 header text -> cleaned section body."""
    m = re.search(r"<main[^>]*>(.*?)</main>", html, re.DOTALL | re.IGNORECASE)
    if not m:
        return {}
    main = m.group(1) + "</main>"
    sections = {}
    for sec in H2_SECTION_RE.finditer(main):
        header = clean_text(sec.group(1))
        body = clean_text(sec.group(2))
        if header and body:
            sections[header] = body
    return sections


def parse_index(idx_html: str) -> list[tuple[str, str]]:
    pat = re.compile(r'<a [^>]*href="/health/([a-z0-9\-]+)"[^>]*>([^<]+)</a>',
                     re.IGNORECASE)
    seen = set()
    out = []
    for m in pat.finditer(idx_html):
        slug, name = m.group(1), clean_text(m.group(2))
        if slug in NON_HERB_SLUGS:
            continue
        if slug in seen:
            continue
        seen.add(slug)
        out.append((slug, name))
    return out


SECTION_MAP = {
    "Background": "summary",
    "How Much Do We Know?": "research_extent",
    "What Have We Learned?": "findings",
    "What Do We Know About Safety?": "safety",
    "Bottom Line": "bottom_line",
    "What Have Studies Shown?": "findings",
}


def infer_rating(findings: str) -> str:
    """Heuristic: classify the strength of NCCIH evidence wording."""
    if not findings:
        return "insufficient_data"
    f = findings.lower()
    # Order matters — strongest cues first
    if re.search(r"\bno (good|reliable|conclusive|convincing) evidence\b", f) or \
       re.search(r"hasn'?t been (shown|found) to (work|help|be effective)", f) or \
       re.search(r"isn'?t effective", f):
        return "no_evidence"
    if re.search(r"\binsufficient evidence\b|\btoo few studies\b|\bnot enough (research|studies)\b", f):
        return "insufficient_data"
    if re.search(r"\bmay (help|reduce|improve|be helpful)\b|some studies suggest|preliminary evidence", f):
        return "weak_support"
    if re.search(r"effective for|moderate evidence|consistently show", f):
        return "moderate_support"
    if re.search(r"strong evidence|well established|proven", f):
        return "strong_support"
    return "insufficient_data"


def main() -> None:
    idx_html = fetch(INDEX_URL, RAW_DIR / "_index.html")
    entries = parse_index(idx_html)
    print(f"NCCIH index: {len(entries)} candidate herb entries")

    records = []
    for slug, idx_name in entries:
        url = BASE + slug
        cache = RAW_DIR / f"{slug}.html"
        try:
            page = fetch(url, cache, allow_404=True)
        except Exception as exc:
            print(f"  [!] {slug}: {exc}", file=sys.stderr)
            continue
        if not page:
            continue
        # Skip pages that aren't herb fact sheets (no Background section, etc.)
        sections = extract_sections(page)
        if "Background" not in sections and "What Have We Learned?" not in sections:
            continue
        name, common, latin = extract_meta(page)
        primary = (latin[0] if latin else name) or idx_name
        record = {
            "id": slugify(primary),
            "names": {
                "primary": primary,
                "latin": latin[0] if latin else primary,
                "common": common,
                "synonyms": latin[1:] if len(latin) > 1 else [],
            },
            "category": "herbal",
            "evidence": {
                "summary": sections.get("Background", "")[:4000],
                "studies": [],
                "documented_adverse_effects": [],
                "regulatory": ["NCCIH (US National Center for Complementary and Integrative Health) factsheet"],
            },
            "provenance": {"sources": [SOURCE_ID]},
        }
        ev = record["evidence"]
        for h2, body in sections.items():
            mapped = SECTION_MAP.get(h2)
            if not mapped:
                continue
            if mapped == "summary":
                continue  # already set
            if mapped in ("research_extent", "findings", "bottom_line"):
                ev["studies"].append({
                    "section": h2,
                    "text": body[:6000],
                    "source": "NCCIH",
                })
            elif mapped == "safety":
                ev["documented_adverse_effects"].append({
                    "text": body[:6000],
                    "source": "NCCIH",
                })

        # Heuristic rating from findings text
        findings_text = " ".join(s["text"] for s in ev["studies"] if "text" in s)
        ev["overall_rating"] = infer_rating(findings_text)

        records.append(record)
        print(f"  + {slug:25s} -> {primary} ({ev['overall_rating']})")

    out = PROCESSED_DIR / "nccih.json"
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(records)} records -> {out}")


if __name__ == "__main__":
    main()
