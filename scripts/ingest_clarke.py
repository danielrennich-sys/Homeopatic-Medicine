"""
Ingest Clarke's Dictionary of Practical Materia Medica (1900-1902, 3 vols)
from homeoint.org Médi-T digitization. Public domain in US (Clarke d. 1931, pub. 1900).

Clarke's structure:
  - Title + botanical/preparation line (centered after title)
  - "Clinical." section: list of conditions
  - "Characteristics." section: narrative description
  - "Relations." section: complementary/antidoted/compared remedies
  - "Causation." section: known triggers
  - "SYMPTOMS." centered header, followed by ~38 numbered subsections:
      1. Mind. 2. Head. 3. Eyes. 4. Ears. 5. Nose. 6. Face. 7. Teeth.
      8. Mouth. 9. Throat. 10. Appetite. 11. Stomach. 12. Abdomen.
      13. Stool and Anus. 14. Urinary Organs. 15. Male Sexual Organs.
      16. Female Sexual Organs. 17. Pregnancy/Parturition/Lactation.
      18. Voice/Larynx/Trachea/Bronchia. 19. Respiration. 20. Cough.
      21. Inner Chest/Lungs. 22. Heart/Pulse/Circulation. 23. Outer Chest.
      24. Neck and Back. 25. Upper Limb. 26. Lower Limb. 27. Limbs in General.
      28. Nerves. 29. Sleep and Dreams. 30. Time. 31. Temperature and Weather.
      32. Fever. 33. Locality and Direction. 34. Sensations. 35. Tissues.
      36. Touch/Passive Motion/Injuries. 37. Skin. 38. Stages of Life.

Within sections, individual symptoms are separated by U+2500 (─, &#9472;).
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
RAW_DIR = ROOT / "data" / "raw" / "clarke"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE = "http://www.homeoint.org/clarke/"
SOURCE_ID = "clarke-dictionary-1900"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

# Clarke index uses <a href="<letter>/<slug>.htm" target="_top">DispName</a>.
INDEX_RE = re.compile(
    r'<a href="([a-z])/([a-z0-9_]+)\.htm"[^>]*>([^<]+)</a>',
    re.IGNORECASE,
)

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

# Clarke's numbered subsections under SYMPTOMS — map to our schema
NUMBERED_SECTION_MAP = {
    "mind": "mental_emotional",
    "head": "head",
    "eyes": "eyes",
    "ears": "ears",
    "nose": "nose",
    "face": "face",
    "teeth": "mouth_throat",
    "mouth": "mouth_throat",
    "throat": "mouth_throat",
    "appetite": "stomach_abdomen",
    "stomach": "stomach_abdomen",
    "abdomen": "stomach_abdomen",
    "stool and anus": "stool_rectum",
    "stool": "stool_rectum",
    "anus": "stool_rectum",
    "urinary organs": "urinary",
    "urinary": "urinary",
    "male sexual organs": "male",
    "male": "male",
    "female sexual organs": "female",
    "female": "female",
    "pregnancy": "female",
    "pregnancy, parturition, lactation": "female",
    "voice and larynx, trachea and bronchia": "respiratory",
    "voice": "respiratory",
    "larynx": "respiratory",
    "respiration": "respiratory",
    "cough": "respiratory",
    "inner chest and lungs": "chest_heart",
    "lungs": "chest_heart",
    "chest": "chest_heart",
    "heart, pulse and circulation": "chest_heart",
    "heart": "chest_heart",
    "outer chest": "chest_heart",
    "neck and back": "back",
    "back": "back",
    "neck": "back",
    "upper limb": "extremities",
    "lower limb": "extremities",
    "limbs in general": "extremities",
    "extremities": "extremities",
    "nerves": "generalities",
    "sleep and dreams": "sleep",
    "sleep": "sleep",
    "time": "modalities",
    "temperature and weather": "modalities",
    "fever": "fever_chill",
    "locality and direction": "generalities",
    "sensations": "generalities",
    "tissues": "generalities",
    "touch, passive motion, injuries": "generalities",
    "touch": "generalities",
    "skin": "skin",
    "stages of life and constitution": "generalities",
    "stages of life": "generalities",
}


def fetch(url: str, cache_path: Path) -> str:
    if cache_path.exists():
        raw = cache_path.read_bytes()
    else:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, context=CTX, timeout=30) as r:
            raw = r.read()
        cache_path.write_bytes(raw)
        time.sleep(0.5)
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
    s = name.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def split_bullets(text: str) -> list[str]:
    """Clarke uses ─ (U+2500) as in-section symptom separator."""
    parts = re.split(r"[\u2500]+", text)
    return [p.strip(" .,;") for p in parts if p.strip(" .,;")]


def parse_remedy(letter: str, slug: str, html: str, fallback_name: str) -> dict:
    # Title
    m = re.search(
        r'<font size="5"[^>]*color="#800000"[^>]*>\s*<p[^>]*>([^<]+)</p>',
        html, re.IGNORECASE,
    )
    title = clean_text(m.group(1)) if m else fallback_name
    title = title.rstrip(".").strip()

    # Convert &#9472; to U+2500 for splitting later
    text_html = html.replace("&#9472;", "\u2500")
    plain = clean_text(text_html)

    # Find boundaries of major sections by their literal markers in the cleaned text
    def section_between(start_marker: str, end_markers: list[str]) -> str:
        i = plain.find(start_marker)
        if i == -1:
            return ""
        i += len(start_marker)
        end = len(plain)
        for em in end_markers:
            j = plain.find(em, i)
            if j != -1 and j < end:
                end = j
        return plain[i:end].strip()

    clinical = section_between("Clinical.\u2500", ["Characteristics.", "Relations.", "Causation.", "SYMPTOMS."])
    if not clinical:
        clinical = section_between("Clinical.", ["Characteristics.", "Relations.", "Causation.", "SYMPTOMS."])
    characteristics = section_between("Characteristics.\u2500", ["Relations.", "Causation.", "SYMPTOMS."])
    if not characteristics:
        characteristics = section_between("Characteristics.", ["Relations.", "Causation.", "SYMPTOMS."])
    relations = section_between("Relations.\u2500", ["Causation.", "SYMPTOMS."])
    if not relations:
        relations = section_between("Relations.", ["Causation.", "SYMPTOMS."])
    causation = section_between("Causation.\u2500", ["SYMPTOMS."])
    if not causation:
        causation = section_between("Causation.", ["SYMPTOMS."])

    # Numbered SYMPTOMS subsections
    sym_start = plain.find("SYMPTOMS.")
    symptoms_block = plain[sym_start + len("SYMPTOMS."):] if sym_start != -1 else ""
    # Pattern: " 1. Name." through " 38. Name."
    numbered_re = re.compile(r"\s(\d{1,2})\.\s+([A-Z][A-Za-z ,/&;]{2,60}?)\.\u2500")
    numbered_matches = list(numbered_re.finditer(symptoms_block))

    sections_by_field: dict[str, list[str]] = {}
    for i, m in enumerate(numbered_matches):
        label = m.group(2).strip().lower()
        start = m.end()
        end = numbered_matches[i + 1].start() if i + 1 < len(numbered_matches) else len(symptoms_block)
        body = symptoms_block[start:end]
        bullets = split_bullets(body)
        # Map label to schema field; try exact, then strip trailing words
        field = NUMBERED_SECTION_MAP.get(label)
        if field is None:
            # Try first 1-2 words
            short = " ".join(label.split()[:2])
            field = NUMBERED_SECTION_MAP.get(short)
        if field is None:
            short = label.split()[0]
            field = NUMBERED_SECTION_MAP.get(short, "generalities")
        sections_by_field.setdefault(field, []).extend(bullets)

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

    if characteristics:
        trad["keynotes"] = [{"text": characteristics, "source_id": SOURCE_ID}]

    if clinical:
        # Clinical is a list separated by . — store under generalities with a label
        items = [c.strip(" .,;") for c in re.split(r"\.\s+(?=[A-Z])|\u2500", clinical) if c.strip(" .,;")]
        if items:
            trad.setdefault("generalities", []).append({
                "text": "Clinical: " + "; ".join(items),
                "source_id": SOURCE_ID,
            })

    if relations:
        trad["relationships"] = {"_raw": [relations]}

    if causation:
        trad.setdefault("generalities", []).append({
            "text": "Causation: " + causation, "source_id": SOURCE_ID,
        })

    for field, bullets in sections_by_field.items():
        if not bullets:
            continue
        merged = ". ".join(bullets) + "."
        trad.setdefault(field, []).append({"text": merged, "source_id": SOURCE_ID})

    return record


def main(letters: str | None = None) -> None:
    letters_to_do = letters.lower() if letters else string.ascii_lowercase
    all_records: list[dict] = []
    for letter in letters_to_do:
        idx_url = f"{BASE}{letter}.htm"
        idx_cache = RAW_DIR / f"{letter}.htm"
        try:
            idx_html = fetch(idx_url, idx_cache)
        except Exception as e:
            print(f"[!] index {letter}: {e}", file=sys.stderr)
            continue
        # Match links in this letter only
        entries = []
        for m in INDEX_RE.finditer(idx_html):
            ltr, slug, name = m.groups()
            if ltr.lower() != letter:
                continue
            entries.append((ltr.lower(), slug.lower(), clean_text(name)))
        # Dedup (some appear twice in nav)
        seen = set()
        uniq = []
        for e in entries:
            if e[1] in seen:
                continue
            seen.add(e[1])
            uniq.append(e)
        print(f"[{letter}] {len(uniq)} remedies")
        for ltr, slug, name in uniq:
            url = f"{BASE}{ltr}/{slug}.htm"
            cache = RAW_DIR / ltr / f"{slug}.htm"
            try:
                page = fetch(url, cache)
            except Exception as exc:
                print(f"  [!] {slug}: {exc}", file=sys.stderr)
                continue
            try:
                rec = parse_remedy(ltr, slug, page, name)
                all_records.append(rec)
            except Exception as exc:
                print(f"  [!] parse {slug}: {exc}", file=sys.stderr)

    out = PROCESSED_DIR / "clarke.json"
    out.write_text(json.dumps(all_records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote {len(all_records)} records -> {out}")


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(arg)
