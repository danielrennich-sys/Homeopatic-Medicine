"""
Ingest Hahnemann's Materia Medica Pura (1830, Dudgeon translation) from the
archive.org alpha-sorted compilation:

  identifier: materia-medica-pura-vols-1-6-alpha-sort-1185pp-samuel-hahnemann_202310
  file:       Materia medica pura vols 1-6 (alpha sort) 1185pp-Samuel Hahnemann_djvu.txt

Hahnemann d. 1843, original German edition pre-1843, Dudgeon translation 1880,
Hughes annotations 1881 — all pre-1929, public domain in US.

Structure of the OCR text:
  - Volumes 1-6 are concatenated and re-sorted alphabetically by remedy.
  - Each remedy section begins with a Latin-name header line (e.g.
    "Aconitum nappelus") immediately followed by common-name parenthetical and
    a "{abbrev} [f-h1]" page-anchor marker. The same {abbrev} marker recurs at
    the end of every paragraph in that remedy's section, so contiguous blocks
    sharing one abbreviation belong to the same remedy.
  - Symptoms are individually numbered (e.g. "102. A pain in the left ...").
  - OCR introduces a few corrupted abbreviations (e.g. "am" for "arn",
    "eycel"/"cyel" for "cycl") which we drop / merge.

Strategy:
  1. Tokenise all `{abbrev}` occurrences.
  2. For each abbrev appearing in `>= MIN_OCCURRENCES` paragraphs and not in
     a known OCR-typo blacklist, record its first occurrence position.
  3. Sort first-occurrence positions and slice the text between consecutive
     boundaries.
  4. From each slice, pull the Latin-name header (last short, capitalised
     line preceding the first marker in the slice) and the body.
  5. Clean: drop "{abbrev}" tags, "[f-h1]"/"[L.r]" source citations,
     join broken lines, and split into numbered symptoms.
  6. Emit one record per remedy keyed by the abbreviation; the merge step
     resolves abbreviations to canonical Latin via ABBREVIATION_MAP.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / "hahnemann" / "mmp_alphasort.txt"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_ID = "hahnemann-mmp-1830"
MIN_OCCURRENCES = 50

# OCR-corrupted abbreviations -> canonical (or None to drop entirely)
OCR_FIX = {
    "am": "arn",       # arnica
    "eycel": "cycl",
    "cyel": "cycl",
    "thuy": "thuj",
    "thu": "thuj",
    "fh": None,
    "f": None,
    "h": None,
    "l": None,
}

# Some abbrev -> nice latin name (used as fallback when header parse fails).
# These are *display* fallbacks; the merge step also has its own mapping.
ABBREV_TO_LATIN = {
    "acon": "Aconitum napellus",
    "ambr": "Ambra grisea",
    "ang": "Angustura",
    "arg-m": "Argentum metallicum",
    "arn": "Arnica montana",
    "ars": "Arsenicum album",
    "asar": "Asarum europaeum",
    "aur": "Aurum metallicum",
    "bell": "Belladonna",
    "bism": "Bismuthum",
    "bry": "Bryonia alba",
    "camph": "Camphora",
    "caps": "Capsicum",
    "cham": "Chamomilla",
    "chel": "Chelidonium",
    "chin": "China officinalis",
    "cic": "Cicuta virosa",
    "cina": "Cina maritima",
    "cocc": "Cocculus indicus",
    "coloc": "Colocynthis",
    "con": "Conium maculatum",
    "cycl": "Cyclamen europaeum",
    "dig": "Digitalis purpurea",
    "dros": "Drosera rotundifolia",
    "dulc": "Dulcamara",
    "euphr": "Euphrasia officinalis",
    "ign": "Ignatia amara",
    "ip": "Ipecacuanha",
    "led": "Ledum palustre",
    "manc": "Manganum aceticum",
    "men": "Menyanthes trifoliata",
    "merc": "Mercurius vivus",
    "mosch": "Moschus",
    "ph-ac": "Phosphoricum acidum",
    "puls": "Pulsatilla nigricans",
    "rheum": "Rheum",
    "samb": "Sambucus nigra",
    "sambn": "Sambucus nigra",
    "spig": "Spigelia anthelmia",
    "stann": "Stannum metallicum",
    "staph": "Staphysagria",
    "stram": "Stramonium",
    "sulph": "Sulphur",
    "thuj": "Thuja occidentalis",
    "verat": "Veratrum album",
    "verb": "Verbascum",
    "hyos": "Hyoscyamus niger",
    "ign": "Ignatia amara",
    "op": "Opium",
    "rhus": "Rhus toxicodendron",
    "sep": "Sepia",
    "sil": "Silicea",
}

ABBREV_RE = re.compile(r"\{([a-z\-]+)\}")
SOURCE_CITE_RE = re.compile(r"\[[A-Za-z\.\- ]{1,30}\]")  # [Lr.] [Cullen, l.c.]
ANCHOR_RE = re.compile(r"\[f[\.\-]?h1\]")
NUMBERED_RE = re.compile(r"(?:^|\s)(\d{1,4})\.\s+(.+?)(?=(?:\s\d{1,4}\.\s)|\Z)", re.DOTALL)
WS_RE = re.compile(r"\s+")
HEADER_LINE_RE = re.compile(r"^[A-Z][A-Za-z]+(\s+[a-z]+){0,2}\.?$")


def slugify(name: str) -> str:
    s = name.replace("\u00c6", "AE").replace("\u00e6", "ae")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def normalise_abbrev(ab: str) -> str | None:
    if ab in OCR_FIX:
        return OCR_FIX[ab]
    return ab


def extract_remedy_boundaries(text: str) -> list[tuple[int, str]]:
    """Return [(position, canonical_abbrev), ...] sorted by first-occurrence."""
    counts: dict[str, int] = {}
    first_pos: dict[str, int] = {}
    for m in ABBREV_RE.finditer(text):
        ab = normalise_abbrev(m.group(1))
        if not ab:
            continue
        counts[ab] = counts.get(ab, 0) + 1
        if ab not in first_pos:
            first_pos[ab] = m.start()
    boundaries = [
        (pos, ab) for ab, pos in first_pos.items()
        if counts[ab] >= MIN_OCCURRENCES
    ]
    boundaries.sort()
    return boundaries


def clean_body(raw: str) -> str:
    s = ABBREV_RE.sub(" ", raw)
    s = ANCHOR_RE.sub(" ", s)
    s = SOURCE_CITE_RE.sub(" ", s)
    # Join broken lines (but preserve paragraph breaks at blank lines)
    paras = re.split(r"\n\s*\n", s)
    out = []
    for p in paras:
        p = p.replace("\n", " ")
        p = WS_RE.sub(" ", p).strip()
        if p:
            out.append(p)
    return "\n\n".join(out)


def split_numbered_symptoms(body: str) -> list[str]:
    """Hahnemann numbers each symptom (1-2000+ per remedy)."""
    matches = list(NUMBERED_RE.finditer(body))
    if not matches:
        return []
    out = []
    for m in matches:
        text = m.group(2)
        text = WS_RE.sub(" ", text).strip(" .,;")
        if 5 < len(text) < 1500:
            out.append(text)
    return out


def extract_header_name(slice_text: str, abbrev: str) -> str:
    """Look at the first ~500 chars; the Latin name header is one of the
    first short capitalised lines."""
    head = slice_text[:600]
    for line in head.splitlines():
        s = line.strip()
        if not s:
            continue
        if HEADER_LINE_RE.match(s) and len(s) > 3 and len(s) < 60:
            # Skip single-letter section dividers like "A", "B"
            if len(s) <= 2:
                continue
            return s.rstrip(".")
    return ABBREV_TO_LATIN.get(abbrev, abbrev.capitalize())


def main() -> None:
    if not RAW_PATH.exists():
        print(f"missing {RAW_PATH}", file=sys.stderr)
        sys.exit(1)
    text = RAW_PATH.read_text(encoding="utf-8", errors="replace")

    boundaries = extract_remedy_boundaries(text)
    print(f"detected {len(boundaries)} remedy sections")

    records = []
    for i, (pos, abbrev) in enumerate(boundaries):
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(text)
        slice_text = text[pos:end]
        # Step back a little to capture the header line that often sits just
        # before the first {abbrev} marker
        header_back_start = max(0, pos - 200)
        header_zone = text[header_back_start:pos]
        latin = extract_header_name(header_zone + "\n" + slice_text[:600], abbrev)

        body = clean_body(slice_text)
        if len(body) < 200:
            continue

        symptoms = split_numbered_symptoms(body)
        # Pull a non-numbered intro paragraph (the prefatory note before symptom 1)
        first_num = NUMBERED_RE.search(body)
        intro = body[:first_num.start()].strip() if first_num else body[:1500]
        intro = WS_RE.sub(" ", intro).strip()

        record = {
            "id": slugify(latin),
            "names": {
                "primary": latin,
                "latin": latin,
            },
            "category": "homeopathic",
            "traditional": {},
            "provenance": {"sources": [SOURCE_ID]},
        }
        trad = record["traditional"]
        if intro and len(intro) > 30:
            trad["keynotes"] = [{"text": intro[:4000], "source_id": SOURCE_ID}]
        if symptoms:
            # Hahnemann's provings are pure symptom lists — file under generalities
            # so the merge step can route them.
            joined = ". ".join(symptoms)
            trad["generalities"] = [{"text": joined, "source_id": SOURCE_ID}]
        if not trad:
            continue
        # Drop obvious junk slices (TOC pages, fragments)
        if record["id"] in {"contents", "preface", "introduction"}:
            continue
        gen_chars = len(trad.get("generalities", [{}])[0].get("text", "")) if "generalities" in trad else 0
        if gen_chars < 3000:
            continue
        records.append(record)

    out = PROCESSED_DIR / "hahnemann_mmp.json"
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(records)} records -> {out}")


if __name__ == "__main__":
    main()
