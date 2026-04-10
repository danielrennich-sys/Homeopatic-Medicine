"""
Ingest NIH Office of Dietary Supplements (ODS) Health Professional fact sheets.

Source: https://ods.od.nih.gov/factsheets/list-all/
We pick the -HealthProfessional variants (richer than -Consumer), skip the
Spanish duplicates and topic-survey pages, and route content into the
`evidence` block. ODS sheets cover vitamins, minerals and a handful of
botanicals (Black Cohosh, Ashwagandha, Ephedra, Garlic, Saw Palmetto, etc.).

Section structure (typical):
  Introduction        -> evidence.summary
  Recommended Intakes -> evidence.dosing
  <Topic> and <X>     -> evidence.studies (one per topic h2)
  Health Risks        -> evidence.documented_adverse_effects
  Interactions...     -> evidence.documented_interactions
  References          -> evidence.references_count

We tag botanicals as category="herbal", everything else as "supplement".
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
RAW_DIR = ROOT / "data" / "raw" / "ods"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://ods.od.nih.gov"
INDEX_URL = BASE + "/factsheets/list-all/"
SOURCE_ID = "ods-nih"

CTX = ssl.create_default_context()
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SCRIPT_RE = re.compile(r"<script.*?</script>", re.DOTALL | re.IGNORECASE)
STYLE_RE = re.compile(r"<style.*?</style>", re.DOTALL | re.IGNORECASE)
NAV_RE = re.compile(r"<nav[^>]*>.*?</nav>", re.DOTALL | re.IGNORECASE)
H2_SECTION_RE = re.compile(
    r"<h2[^>]*>(.*?)</h2>(.*?)(?=<h2[^>]*>|<footer|</main>)",
    re.DOTALL | re.IGNORECASE,
)

# Slugs to skip — generic topic surveys, food databases, navigation
SKIP_SLUGS_PARTIAL = (
    "WeightLoss", "ExerciseAndAthletic", "WYNTK", "Background",
    "DietarySupplements", "DietarySupplementsForOlder", "MyDS", "RecommendedIntakes",
    "FrequencyandReferences", "MVMS", "Cancer-",
    "BotanicalBackground", "List-All", "ImmuneFunction", "Memory",
    "Bone", "DSLD", "BMD", "ClinicalTrial",
    "DietarySupplementsInTheTimeOfCOVID19", "COVID19",
)

# Botanical names that should be category=herbal not supplement
BOTANICAL_NAMES = {
    "ashwagandha", "black cohosh", "echinacea", "garlic", "ephedra",
    "saw palmetto", "valerian", "ginger", "turmeric", "ginseng",
    "ginkgo", "milk thistle", "feverfew", "kava", "st johns wort",
    "ginkgo biloba", "wormwood", "yohimbe",
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
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def clean_text(s: str) -> str:
    s = SCRIPT_RE.sub(" ", s)
    s = STYLE_RE.sub(" ", s)
    s = NAV_RE.sub(" ", s)
    s = unescape(TAG_RE.sub(" ", s))
    s = WS_RE.sub(" ", s).strip()
    return s


def parse_index(html: str) -> list[tuple[str, str]]:
    """Return [(factsheet_slug, displayed_name), ...] for HealthProfessional sheets."""
    pat = re.compile(
        r'<a [^>]*href="(/factsheets/[A-Za-z0-9_\-]+)"[^>]*>([^<]+)</a>',
        re.IGNORECASE,
    )
    seen = set()
    out = []
    for m in pat.finditer(html):
        href, name = m.group(1), clean_text(m.group(2))
        slug = href.rsplit("/", 1)[-1]
        if not slug.endswith("-HealthProfessional"):
            continue
        if any(s in slug for s in SKIP_SLUGS_PARTIAL):
            continue
        if slug in seen:
            continue
        seen.add(slug)
        out.append((slug, name))
    return out


def extract_title(html: str) -> str:
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL | re.IGNORECASE)
    if not h1:
        return ""
    return clean_text(h1.group(1)).split(" - ")[0].split(":")[0].strip()


def extract_sections(html: str) -> dict[str, str]:
    m = re.search(r"<main[^>]*>(.*?)</main>", html, re.DOTALL | re.IGNORECASE)
    body = m.group(1) if m else html
    body += "</main>"
    out = {}
    for sec in H2_SECTION_RE.finditer(body):
        header = clean_text(sec.group(1))
        text = clean_text(sec.group(2))
        if header and text and len(text) > 30:
            out[header] = text
    return out


def main() -> None:
    idx_html = fetch(INDEX_URL, RAW_DIR / "_index.html")
    entries = parse_index(idx_html)
    print(f"ODS index: {len(entries)} HealthProfessional fact sheets")

    records = []
    for slug, idx_name in entries:
        url = BASE + "/factsheets/" + slug + "/"
        cache = RAW_DIR / f"{slug}.html"
        try:
            page = fetch(url, cache, allow_404=True)
        except Exception as exc:
            print(f"  [!] {slug}: {exc}", file=sys.stderr)
            continue
        if not page:
            continue
        sections = extract_sections(page)
        if not sections:
            continue
        title = extract_title(page) or idx_name.split(" - ")[0]
        title = title.strip()

        is_botanical = any(b in title.lower() for b in BOTANICAL_NAMES)
        category = "herbal" if is_botanical else "supplement"

        record = {
            "id": slugify(title),
            "names": {
                "primary": title,
                "latin": title,
            },
            "category": category,
            "evidence": {
                "summary": sections.get("Introduction", "")[:4000],
                "studies": [],
                "documented_adverse_effects": [],
                "documented_interactions": [],
                "regulatory": [
                    "NIH Office of Dietary Supplements (ODS) Health Professional fact sheet"
                ],
            },
            "provenance": {"sources": [SOURCE_ID]},
        }
        ev = record["evidence"]
        for h2, text in sections.items():
            hl = h2.lower()
            if hl in {"introduction", "table of contents", "disclaimer", "references"}:
                continue
            if "health risk" in hl or "adverse" in hl or "toxicity" in hl:
                ev["documented_adverse_effects"].append({
                    "section": h2, "text": text[:8000], "source": "ODS-NIH",
                })
            elif "interaction" in hl:
                ev["documented_interactions"].append({
                    "section": h2, "text": text[:8000], "source": "ODS-NIH",
                })
            elif "recommended intake" in hl or "intake" in hl or "sources of" in hl:
                ev.setdefault("dosing", []).append({
                    "section": h2, "text": text[:6000], "source": "ODS-NIH",
                })
            else:
                ev["studies"].append({
                    "section": h2, "text": text[:8000], "source": "ODS-NIH",
                })

        records.append(record)
        print(f"  + {slug}")

    out = PROCESSED_DIR / "ods.json"
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(records)} records -> {out}")


if __name__ == "__main__":
    main()
