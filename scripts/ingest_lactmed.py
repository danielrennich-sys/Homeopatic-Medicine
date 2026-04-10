"""
Ingest LactMed (Drugs and Lactation Database) herb/supplement entries from
NIH NLM Bookshelf.

Source: https://www.ncbi.nlm.nih.gov/books/NBK501922/
Public domain (US federal government work).

We only ingest herbal, botanical, vitamin, mineral and dietary supplement
entries — not pharmaceutical drugs. Each entry goes into the `evidence`
block with pregnancy/lactation safety data.
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
RAW_DIR = ROOT / "data" / "raw" / "lactmed"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_ID = "lactmed-nih"
CTX = ssl.create_default_context()
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SCRIPT_RE = re.compile(r"<script.*?</script>", re.DOTALL | re.IGNORECASE)
STYLE_RE = re.compile(r"<style.*?</style>", re.DOTALL | re.IGNORECASE)

# Herb/supplement entries identified from the LactMed index (LM codes + named slugs)
HERB_ENTRIES = [
    "LM915",  # Aloe
    "LM916",  # Alfalfa
    "LM868",  # Anise
    "LM914",  # Arnica
    "LM971",  # Astragalus
    "LM912",  # Basil
    "LM913",  # Barley
    "LM943",  # Belladonna
    "LM942",  # Betony
    "LM911",  # Bilberry
    "LM972",  # Bitter Orange
    "LM872",  # Black Cohosh
    "LM958",  # Black Seed
    "LM867",  # Blessed Thistle
    "LM871",  # Blue Cohosh
    "LM880",  # Borage
    "LM941",  # Buckthorn
    "LM866",  # Cabbage
    "LM940",  # Calendula
    "LM881",  # Caraway
    "LM973",  # Carrot
    "LM957",  # Castor
    "LM897",  # Chamomile
    "LM896",  # Chasteberry
    "LM909",  # Chlorella
    "cinnamon",  # Cinnamon
    "LM956",  # Coleus
    "LM865",  # Comfrey
    "LM882",  # Coriander
    "LM922",  # Cranberry
    "LM955",  # Cumin
    "LM954",  # Dandelion
    "LM959",  # Dill
    "LM921",  # Dong Quai
    "LM899",  # Echinacea
    "LM895",  # Eleuthero
    "LM920",  # Elderberry
    "LM974",  # Evening Primrose
    "LM964",  # Euphorbia
    "LM883",  # Fennel
    "LM870",  # Fenugreek
    "LM944",  # Feverfew
    "LM975",  # Flaxseed
    "LM873",  # Garlic
    "LM918",  # Geranium
    "LM877",  # Ginger
    "LM898",  # Ginkgo
    "LM901",  # Ginseng
    "LM904",  # Goat's Rue
    "LM949",  # Goldenseal
    "LM931",  # Green Tea
    "LM917",  # Hawthorn
    "LM963",  # Hibiscus
    "LM919",  # Hops
    "LM928",  # Jasmine
    "LM926",  # Lemon Balm
    "LM925",  # Licorice
    "LM924",  # Marshmallow
    "LM863",  # Milk Thistle
    "LM979",  # Moringa
    "LM923",  # Nutmeg
    "LM929",  # Oregano
    "LM962",  # Papaya
    "LM961",  # Parsley
    "LM935",  # Peppermint
    "LM981",  # Peony
    "LM945",  # Rhubarb
    "LM903",  # Sage
    "LM474",  # Senna
    "LM934",  # Seaweed
    "LM933",  # Spirulina
    "LM862",  # St. John's Wort
    "LM869",  # Stinging Nettle
    "LM965",  # Tea Tree Oil
    "LM930",  # Turmeric
    "LM982",  # Uva Ursi
    "LM902",  # Valerian
    "LM936",  # Vervain
    "LM983",  # Willow Bark
    "LM984",  # Withania (Ashwagandha)
    # Vitamins/minerals/supplements
    "LM985",  # Beta-Carotene
    "LM879",  # Coenzyme Q10
    "LM937",  # Creatine
    "LM977",  # Glucomannan
    "LM950",  # Glucosamine
    "LM591",  # Iodine
    "iron_salts",  # Iron
    "LM946",  # Melatonin
    "LM952",  # SAM-e
    "LM1389",  # Vitamin A
    "pyridoxine",  # Vitamin B6
    "LM1437",  # Vitamin B12
    "LM1492",  # Vitamin C
    "LM1323",  # Vitamin D
    "LM1357",  # Vitamin E
    "LM1330",  # Vitamin K
    "zinc",  # Zinc
    "LM1098",  # Black Currant Seed Oil
    "brewers_yeast",  # Brewer's Yeast
    "LM939",  # Chondroitin
    "LM976",  # Garcinia
    "LM864",  # Lecithin
    "LM978",  # Marine Oils
    "LM960",  # Resveratrol
    "theanine",  # Theanine
]

BASE_URL = "https://www.ncbi.nlm.nih.gov/books/n/lactmed/"


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


def main() -> None:
    records = []
    for entry_id in HERB_ENTRIES:
        url = BASE_URL + entry_id + "/"
        cache = RAW_DIR / f"{entry_id}.html"
        try:
            page = fetch(url, cache)
        except Exception as exc:
            print(f"  [!] {entry_id}: {exc}", file=sys.stderr)
            continue
        if not page:
            continue
        title = extract_title(page)
        if not title:
            continue
        sections = extract_sections(page)
        if not sections:
            # Try broader body extraction
            body_text = clean_text(page)
            if len(body_text) > 300:
                sections = {"Summary": body_text[:8000]}
            else:
                continue

        is_vitamin = any(kw in title.lower() for kw in
                        ["vitamin", "iron", "zinc", "iodine", "selenium"])
        is_herbal = not is_vitamin

        record = {
            "id": slugify(title),
            "names": {
                "primary": title,
                "latin": title,
            },
            "category": "herbal" if is_herbal else "supplement",
            "evidence": {
                "summary": "",
                "lactation_safety": [],
                "documented_adverse_effects": [],
                "regulatory": ["LactMed - NIH Drugs and Lactation Database"],
            },
            "provenance": {"sources": [SOURCE_ID]},
        }
        ev = record["evidence"]

        for hdr, text in sections.items():
            hl = hdr.lower()
            if "summary" in hl or "use during lactation" in hl:
                ev["summary"] = text[:6000]
                ev["lactation_safety"].append({
                    "section": hdr, "text": text[:8000], "source": "LactMed-NIH",
                })
            elif "drug level" in hl or "effects in breastfed" in hl or "effect" in hl:
                ev["lactation_safety"].append({
                    "section": hdr, "text": text[:8000], "source": "LactMed-NIH",
                })
            elif "adverse" in hl or "safety" in hl or "risk" in hl:
                ev["documented_adverse_effects"].append({
                    "section": hdr, "text": text[:8000], "source": "LactMed-NIH",
                })
            else:
                ev.setdefault("studies", []).append({
                    "section": hdr, "text": text[:6000], "source": "LactMed-NIH",
                })

        records.append(record)
        print(f"  + {entry_id} -> {title}")

    out = PROCESSED_DIR / "lactmed.json"
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(records)} records -> {out}")


if __name__ == "__main__":
    main()
