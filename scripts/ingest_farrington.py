"""
Ingest Farrington's Clinical Materia Medica (4th ed., 1908) from the
archive.org OCR text:

  identifier: clinicalmateria00farr
  file:       clinicalmateria00farr_djvu.txt

Farrington died 1885; original lectures published 1887 (Bartlett),
4th edition 1908 — all public domain in US.

Structure:
  - 73 LECTURES, headed by `LECTURE <ROMAN>` then a topic header line
    (e.g. "CANTHARIS." or "ANIMAL KINGDOM" or "THE OPHIDIA").
  - Some lectures cover one remedy by name, others cover an entire family
    (Ophidia, Arachnida, Hymenoptera). We slice per lecture and use the
    topic header as the primary name; multi-remedy lectures retain their
    family name and the merge step will not falsely collapse them.
  - Within a lecture, individual remedies are introduced by ALL-CAPS or
    capitalised remedy names. We don't try to split sub-sections here —
    keep the lecture as one prose chunk and let downstream analysis or a
    future refinement script extract sub-remedies.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW_PATH = ROOT / "data" / "raw" / "farrington" / "clinical_mm.txt"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SOURCE_ID = "farrington-clinical-1908"

LECTURE_RE = re.compile(r"\bLECTURE\s+([IVXLCDM]+)\b\.?", re.IGNORECASE)
WS_RE = re.compile(r"\s+")
PAGE_NO_RE = re.compile(r"\n\s*\d{1,4}\s*\n")  # standalone page numbers


def slugify(name: str) -> str:
    s = unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def extract_lecture_title(slice_text: str) -> str:
    """The topic title is the first all-caps line after the LECTURE marker."""
    # Skip the LECTURE I. line and look at the next non-blank line
    lines = slice_text.splitlines()
    started = False
    for ln in lines:
        s = ln.strip().rstrip(".")
        if not s:
            continue
        if not started:
            # Skip the LECTURE marker line
            if re.match(r"LECTURE\s+[IVXLCDM]+", s, re.IGNORECASE):
                started = True
                continue
            started = True  # corrupt header — start scanning anyway
        # Title heuristic: short, mostly upper-case
        if 3 < len(s) < 60 and re.search(r"[A-Z]", s):
            uppers = sum(1 for c in s if c.isupper())
            letters = sum(1 for c in s if c.isalpha())
            if letters > 0 and uppers / letters > 0.7:
                return s
        break
    return ""


def clean_body(text: str) -> str:
    text = PAGE_NO_RE.sub("\n", text)
    # Join words split across line breaks (hyphenated)
    text = re.sub(r"-\n\s*", "", text)
    # Collapse newlines to spaces, preserve double-newline paragraph breaks
    paras = re.split(r"\n\s*\n", text)
    out = []
    for p in paras:
        p = p.replace("\n", " ")
        p = WS_RE.sub(" ", p).strip()
        if len(p) > 20:
            out.append(p)
    return "\n\n".join(out)


def main() -> None:
    if not RAW_PATH.exists():
        print(f"missing {RAW_PATH}", file=sys.stderr)
        sys.exit(1)
    text = RAW_PATH.read_text(encoding="utf-8", errors="replace")

    # Strip the front matter (anything before LECTURE I)
    matches = list(LECTURE_RE.finditer(text))
    if not matches:
        print("no LECTURE markers found", file=sys.stderr)
        sys.exit(1)
    print(f"found {len(matches)} lecture markers")

    records = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        slice_text = text[start:end]
        title = extract_lecture_title(slice_text)
        if not title:
            continue
        # Skip lectures whose title is too generic to be a remedy
        if title.upper() in {"INTRODUCTORY", "GENERAL", "REVIEW",
                              "INDEX", "ANIMAL KINGDOM", "VEGETABLE KINGDOM",
                              "MINERAL KINGDOM"}:
            continue
        body = clean_body(slice_text)
        if len(body) < 500:
            continue

        # Title-case the title (it's all caps from OCR)
        primary = title.title()
        record = {
            "id": slugify(primary),
            "names": {
                "primary": primary,
                "latin": primary,
            },
            "category": "homeopathic",
            "traditional": {
                "keynotes": [{"text": body[:30000], "source_id": SOURCE_ID}],
            },
            "provenance": {"sources": [SOURCE_ID]},
        }
        records.append(record)

    out = PROCESSED_DIR / "farrington.json"
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(records)} records -> {out}")


if __name__ == "__main__":
    main()
