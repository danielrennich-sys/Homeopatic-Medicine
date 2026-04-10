"""
Generic ingester for the Médi-T digitized homeopathic books on homeoint.org
that share the same flat-index pattern:

  /<book_path>/index.htm     -> list of <a href="slug.htm">name</a>
  /<book_path>/<slug>.htm    -> remedy entry (full prose, sometimes with
                                 anchored sub-sections)

Books covered:
  - Hahnemann, The Chronic Diseases (1830, Tafel translation 1896)
  - Lippe, Text Book of Materia Medica (1866)
  - Cowperthwaite, A Text-Book of Materia Medica (1891)
  - Hering Condensed Materia Medica
  - Boericke's Repertory (filed under remedies in books4/boerirep)
  - Dewey, Practical Homeopathic Therapeutics (chapter-organized; skipped)
  - Boger Synoptic Key
  - Allen's Handbook of Materia Medica
  - Allen's Clinical Materia Medica
  - Guernsey, Keynotes
  - Roberts, Sensations As If
  - Gentry, Concordance Repertory (skipped — repertory)
  - Nash Therapeutics

Each book becomes its own per-source JSON in data/processed/.
The page parser is conservative: drop nav/menu, take the longest text
block, derive a Latin/title from the page <title> tag.
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
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SCRIPT_RE = re.compile(r"<script.*?</script>", re.DOTALL | re.IGNORECASE)
STYLE_RE = re.compile(r"<style.*?</style>", re.DOTALL | re.IGNORECASE)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.DOTALL | re.IGNORECASE)

# Slug names that are navigation, not remedies
NAV_SLUGS = {
    "index", "frindex", "preface", "intro", "introduction", "contents",
    "abbreviations", "remedies", "biblio", "bibliography", "history",
    "scheme", "translator", "notice", "editor",
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
        time.sleep(0.25)
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
    s = s.replace("\u00a0", " ")
    s = WS_RE.sub(" ", s).strip()
    return s


def parse_index(html: str) -> list[str]:
    """Return slug names (no .htm) for entries in this book."""
    pat = re.compile(r'href="([a-z][a-z0-9_\-]*)\.htm"', re.IGNORECASE)
    seen = set()
    out = []
    for m in pat.finditer(html):
        slug = m.group(1).lower()
        if slug in NAV_SLUGS:
            continue
        if slug in seen:
            continue
        seen.add(slug)
        out.append(slug)
    return out


def parse_title(html: str) -> str:
    m = TITLE_RE.search(html)
    if not m:
        return ""
    title = clean_text(m.group(1))
    # Cut at first " - " (which separates remedy name from book name)
    title = title.split(" - ", 1)[0].strip()
    title = title.rstrip(".")
    return title


def extract_body(html: str) -> str:
    """Strip everything outside <body>, drop nav, return cleaned prose."""
    body_m = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL | re.IGNORECASE)
    body = body_m.group(1) if body_m else html
    # Remove obvious navigation links
    body = re.sub(r'<a [^>]*href="(?:index|frindex)\.htm"[^>]*>.*?</a>',
                  ' ', body, flags=re.IGNORECASE | re.DOTALL)
    # Remove copyright lines
    body = re.sub(r'(?:&copy;|\(c\))\s*M[eé]di-T[^<]*', ' ', body, flags=re.IGNORECASE)
    text = clean_text(body)
    # Heuristic strip of header boilerplate up to first remedy header
    return text


def book_record(slug: str, title: str, body: str, source_id: str,
                category: str = "homeopathic") -> dict | None:
    if not body or len(body) < 200:
        return None
    primary = title or slug
    record = {
        "id": slugify(primary),
        "names": {
            "primary": primary,
            "latin": primary,
        },
        "category": category,
        "traditional": {
            "keynotes": [{"text": body[:30000], "source_id": source_id}],
        },
        "provenance": {"sources": [source_id]},
    }
    return record


def ingest(book_id: str, base_url: str, raw_dir: Path, out_filename: str,
          source_id: str) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    idx_html = fetch(base_url + "index.htm", raw_dir / "_index.html",
                     allow_404=True)
    slugs = parse_index(idx_html) if idx_html else []
    if not slugs:
        # Some books use index.php instead
        idx_php = fetch(base_url + "index.php", raw_dir / "_index_php.html",
                        allow_404=True)
        if idx_php:
            slugs = parse_index(idx_php)
    if not slugs:
        print(f"[{book_id}] no entries found in index", file=sys.stderr)
        return
    print(f"[{book_id}] {len(slugs)} candidate entries")

    records = []
    for slug in slugs:
        url = base_url + f"{slug}.htm"
        cache = raw_dir / f"{slug}.htm"
        try:
            page = fetch(url, cache, allow_404=True)
        except Exception as exc:
            print(f"  [!] {slug}: {exc}", file=sys.stderr)
            continue
        if not page:
            continue
        title = parse_title(page)
        body = extract_body(page)
        rec = book_record(slug, title, body, source_id)
        if rec:
            records.append(rec)

    out = PROCESSED_DIR / out_filename
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Wrote {len(records)} records -> {out}")


CONFIGS = {
    "hahnemann_chronic": {
        "base_url": "http://www.homeoint.org/books/hahchrdi/",
        "raw_dir": ROOT / "data" / "raw" / "hahnemann_chronic",
        "out": "hahnemann_chronic.json",
        "source_id": "hahnemann-chronic-diseases-1896",
    },
    "lippe_mm": {
        "base_url": "http://www.homeoint.org/books1/lippemm/",
        "raw_dir": ROOT / "data" / "raw" / "lippe_mm",
        "out": "lippe_mm.json",
        "source_id": "lippe-textbook-mm-1866",
    },
    "cowperthwaite": {
        "base_url": "http://www.homeoint.org/seror/cowperthwaite/",
        "raw_dir": ROOT / "data" / "raw" / "cowperthwaite",
        "out": "cowperthwaite.json",
        "source_id": "cowperthwaite-textbook-1891",
    },
    "hering_condensed": {
        "base_url": "http://www.homeoint.org/books1/heringcondensed/",
        "raw_dir": ROOT / "data" / "raw" / "hering_condensed",
        "out": "hering_condensed.json",
        "source_id": "hering-condensed-mm-1877",
    },
    "lippe_keynotes": {
        "base_url": "http://www.homeoint.org/books2/lippkeyn/",
        "raw_dir": ROOT / "data" / "raw" / "lippe_keynotes",
        "out": "lippe_keynotes.json",
        "source_id": "lippe-keynotes-1886",
    },
    "boger_synoptic": {
        "base_url": "http://www.homeoint.org/books2/bogersyn/",
        "raw_dir": ROOT / "data" / "raw" / "boger_synoptic",
        "out": "boger_synoptic.json",
        "source_id": "boger-synoptic-key-1915",
    },
    "allen_handbook": {
        "base_url": "http://www.homeoint.org/books1/allenhandbook/",
        "raw_dir": ROOT / "data" / "raw" / "allen_handbook",
        "out": "allen_handbook.json",
        "source_id": "allen-handbook-1889",
    },
    "allen_clinical": {
        "base_url": "http://www.homeoint.org/books2/allenclin/",
        "raw_dir": ROOT / "data" / "raw" / "allen_clinical",
        "out": "allen_clinical.json",
        "source_id": "allen-clinical-mm-1898",
    },
    "guernsey": {
        "base_url": "http://www.homeoint.org/books4/guernsey/",
        "raw_dir": ROOT / "data" / "raw" / "guernsey",
        "out": "guernsey_keynotes.json",
        "source_id": "guernsey-keynotes-1887",
    },
    "roberts": {
        "base_url": "http://www.homeoint.org/books4/roberts/",
        "raw_dir": ROOT / "data" / "raw" / "roberts",
        "out": "roberts_sensations.json",
        "source_id": "roberts-sensations-1937",
    },
    "nash_therap": {
        "base_url": "http://www.homeoint.org/books2/nashtherap/",
        "raw_dir": ROOT / "data" / "raw" / "nash_therap",
        "out": "nash_therapeutics.json",
        "source_id": "nash-therapeutics-1900",
    },
    "kent_mm": {
        "base_url": "http://www.homeoint.org/books3/kentmm/",
        "raw_dir": ROOT / "data" / "raw" / "kent_mm",
        "out": "kent_mm.json",
        "source_id": "kent-mm-1911",
    },
    "kent_newr": {
        "base_url": "http://www.homeoint.org/books2/kentnewr/",
        "raw_dir": ROOT / "data" / "raw" / "kent_newr",
        "out": "kent_new_remedies.json",
        "source_id": "kent-new-remedies-1926",
    },
    "allen_nosodes": {
        "base_url": "http://www.homeoint.org/books1/allennosodes/",
        "raw_dir": ROOT / "data" / "raw" / "allen_nosodes",
        "out": "allen_nosodes.json",
        "source_id": "allen-materia-medica-nosodes-1910",
    },
    "clarke_prescriber": {
        "base_url": "http://www.homeoint.org/books1/clarkeprescriber/",
        "raw_dir": ROOT / "data" / "raw" / "clarke_prescriber",
        "out": "clarke_prescriber.json",
        "source_id": "clarke-prescriber-1895",
    },
    "gentry": {
        "base_url": "http://www.homeoint.org/books1/gentry/",
        "raw_dir": ROOT / "data" / "raw" / "gentry",
        "out": "gentry.json",
        "source_id": "gentry-rubrical-mm-1890",
    },
    "hutch700": {
        "base_url": "http://www.homeoint.org/books2/hutch700/",
        "raw_dir": ROOT / "data" / "raw" / "hutch700",
        "out": "hutchinson_700.json",
        "source_id": "hutchinson-700-1903",
    },
    "arndt": {
        "base_url": "http://www.homeoint.org/books2/arndt/",
        "raw_dir": ROOT / "data" / "raw" / "arndt",
        "out": "arndt.json",
        "source_id": "arndt-system-mm-1885",
    },
    "bidwhow": {
        "base_url": "http://www.homeoint.org/books2/bidwhow/",
        "raw_dir": ROOT / "data" / "raw" / "bidwhow",
        "out": "bidwell_how.json",
        "source_id": "bidwell-how-to-use-1915",
    },
    "boger_genera": {
        "base_url": "http://www.homeoint.org/books5/bogergena/",
        "raw_dir": ROOT / "data" / "raw" / "boger_genera",
        "out": "boger_genera_morborum.json",
        "source_id": "boger-genera-morborum-1907",
    },
    "morgan": {
        "base_url": "http://www.homeoint.org/books5/dewey/",
        "raw_dir": ROOT / "data" / "raw" / "dewey",
        "out": "dewey_essentials.json",
        "source_id": "dewey-essentials-1894",
    },
}


def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    todo = list(CONFIGS) if which == "all" else [which]
    for book in todo:
        if book not in CONFIGS:
            print(f"unknown book: {book}", file=sys.stderr)
            continue
        cfg = CONFIGS[book]
        try:
            ingest(book, cfg["base_url"], cfg["raw_dir"],
                   cfg["out"], cfg["source_id"])
        except Exception as exc:
            print(f"[{book}] FAILED: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
