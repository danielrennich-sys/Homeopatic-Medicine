"""
Ingest LiverTox herbal/dietary supplement hepatotoxicity monographs
from NIH NLM Bookshelf.

Source: https://www.ncbi.nlm.nih.gov/books/NBK547852/
Sub-section: Herbal and Dietary Supplements (NBK548441)
Also: Chinese and Asian Herbal Medicines sub-section

Public domain (US federal government work).
Goes into the `evidence` block — specifically hepatotoxicity / adverse effects.
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
RAW_DIR = ROOT / "data" / "raw" / "livertox"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_ID = "livertox-nih"
INDEX_URL = "https://www.ncbi.nlm.nih.gov/books/n/livertox/HerbalDietarySuppl/"
ASIAN_URL = "https://www.ncbi.nlm.nih.gov/books/n/livertox/ChineseAndAsianHerbs/"

CTX = ssl.create_default_context()
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SCRIPT_RE = re.compile(r"<script.*?</script>", re.DOTALL | re.IGNORECASE)
STYLE_RE = re.compile(r"<style.*?</style>", re.DOTALL | re.IGNORECASE)

# Non-monograph slugs to skip
SKIP_SLUGS = {
    "CaseReport", "Causality", "ClinicalCourse", "DrugPatterns",
    "HerbalDietarySuppl", "ChineseAndAsianHerbs", "aboutus",
    "alert", "beeprod", "disclaimer", "drugliverinjury",
    "editorsandreviewers", "glossary", "intro", "masterlistintro",
    "resource", "Phenotype", "HistologicFindings", "SeverityGrading",
    "Diagnosis", "Management", "SelectedReferences", "NAFLDandDILI",
    "abbreviation", "Contactus", "HepatitisC", "Herbalife",
    "Hydroxycut", "ImmunologicalFeat", "MoveFree", "OxyELITEPro",
    "Slimquick", "PhentypeOfDILI", "SeverityGradDILI", "ClinicalOutcomes",
    "Categorization", "ChloralHydrate", "Crofelemer", "Flavocoxid",
}


def fetch(url: str, cache_path: Path) -> str | None:
    if cache_path.exists():
        raw = cache_path.read_bytes()
    else:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, context=CTX, timeout=30) as r:
                raw = r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                cache_path.write_bytes(b"")
                return None
            raise
        cache_path.write_bytes(raw)
        time.sleep(0.4)
    if not raw:
        return None
    return raw.decode("utf-8", errors="replace")


def clean_text(s: str) -> str:
    s = SCRIPT_RE.sub(" ", s)
    s = STYLE_RE.sub(" ", s)
    s = unescape(TAG_RE.sub(" ", s))
    s = WS_RE.sub(" ", s).strip()
    return s


def slugify(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def extract_title(html: str) -> str:
    m = re.search(r"<title>(.*?)</title>", html, re.DOTALL | re.IGNORECASE)
    if not m:
        return ""
    t = clean_text(m.group(1))
    t = t.split(" - ")[0].strip()
    return t


H2_RE = re.compile(r"<h2[^>]*>(.*?)</h2>(.*?)(?=<h2[^>]*>|</article|</main|</body>)",
                    re.DOTALL | re.IGNORECASE)


def extract_sections(html: str) -> dict[str, str]:
    out = {}
    for m in H2_RE.finditer(html):
        hdr = clean_text(m.group(1))
        body = clean_text(m.group(2))
        if hdr and body and len(body) > 30:
            out[hdr] = body
    return out


def discover_monograph_slugs(index_html: str) -> list[str]:
    links = re.findall(r'href="/books/n/livertox/([A-Za-z][A-Za-z0-9_]+)/"', index_html)
    seen = set()
    out = []
    for slug in links:
        if slug in SKIP_SLUGS or slug in seen:
            continue
        seen.add(slug)
        out.append(slug)
    return out


def main() -> None:
    all_slugs = []
    for idx_url in [INDEX_URL, ASIAN_URL]:
        label = "main" if "Herbal" in idx_url else "asian"
        cache = RAW_DIR / f"_index_{label}.html"
        try:
            page = fetch(idx_url, cache)
        except Exception as e:
            print(f"[!] index {label}: {e}", file=sys.stderr)
            continue
        if page:
            slugs = discover_monograph_slugs(page)
            all_slugs.extend(slugs)
            print(f"LiverTox {label} index: {len(slugs)} entries")

    # Dedupe
    seen = set()
    unique_slugs = []
    for s in all_slugs:
        if s not in seen:
            seen.add(s)
            unique_slugs.append(s)
    print(f"Total unique monograph slugs: {len(unique_slugs)}")

    records = []
    base = "https://www.ncbi.nlm.nih.gov/books/n/livertox/"
    for slug in unique_slugs:
        url = base + slug + "/"
        cache = RAW_DIR / f"{slug}.html"
        try:
            page = fetch(url, cache)
        except Exception as e:
            print(f"  [!] {slug}: {e}", file=sys.stderr)
            continue
        if not page:
            continue
        title = extract_title(page)
        if not title:
            continue
        sections = extract_sections(page)
        if not sections:
            continue

        record = {
            "id": slugify(title),
            "names": {
                "primary": title,
                "latin": title,
            },
            "category": "herbal",
            "evidence": {
                "summary": "",
                "documented_adverse_effects": [],
                "hepatotoxicity": [],
                "regulatory": ["LiverTox - NIH Drug-Induced Liver Injury Database"],
            },
            "provenance": {"sources": [SOURCE_ID]},
        }
        ev = record["evidence"]
        for hdr, text in sections.items():
            hl = hdr.lower()
            if "overview" in hl or "introduction" in hl or "background" in hl:
                ev["summary"] = text[:6000]
            elif "hepatotoxicity" in hl or "liver" in hl or "injury" in hl:
                ev["hepatotoxicity"].append({
                    "section": hdr, "text": text[:8000], "source": "LiverTox-NIH",
                })
            elif "mechanism" in hl:
                ev["hepatotoxicity"].append({
                    "section": hdr, "text": text[:6000], "source": "LiverTox-NIH",
                })
            elif "case" in hl:
                ev.setdefault("case_reports", []).append({
                    "section": hdr, "text": text[:8000], "source": "LiverTox-NIH",
                })
            else:
                ev["documented_adverse_effects"].append({
                    "section": hdr, "text": text[:6000], "source": "LiverTox-NIH",
                })

        records.append(record)
        print(f"  + {slug} -> {title}")

    out = PROCESSED_DIR / "livertox.json"
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(records)} records -> {out}")


if __name__ == "__main__":
    main()
