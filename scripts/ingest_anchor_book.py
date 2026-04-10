"""
Generic ingester for homeoint.org books that pack many remedies into a few pages,
each remedy demarcated by an <a name="anchor"> tag.

Used for:
  - Allen's Keynotes (allkeyn/index.htm)
  - Nash's Leaders in Homoeopathic Therapeutics (nashtherap/index.htm)

Both source authors are public domain in the US (Allen d. 1909, Nash d. 1917).

Strategy:
  1. Fetch the index page; extract all (page, anchor, displayed_name) links.
  2. Download each unique page once.
  3. For each anchor in document order, slice the HTML between this anchor and
     the next; treat that slice as the remedy's body.
  4. Within each slice, extract paragraphs and a centered title.
"""
from __future__ import annotations

import json
import re
import ssl
import sys
import time
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
PARA_RE = re.compile(r"<p\b[^>]*>(.*?)</p>", re.IGNORECASE | re.DOTALL)
ANCHOR_RE = re.compile(r'<a\s+name="([^"]+)"', re.IGNORECASE)
INDEX_LINK_RE = re.compile(
    r'<a href="([a-zA-Z0-9_]+\.htm)#([^"]+)"[^>]*>([^<]+)</a>',
    re.IGNORECASE,
)


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


def slice_remedy_html(page_html: str, anchor: str) -> str:
    """Return the HTML between this anchor and the next anchor on the page."""
    anchors = list(ANCHOR_RE.finditer(page_html))
    for i, m in enumerate(anchors):
        if m.group(1) == anchor:
            start = m.end()
            end = anchors[i + 1].start() if i + 1 < len(anchors) else len(page_html)
            return page_html[start:end]
    return ""


def extract_centered_title(slice_html: str, fallback: str) -> str:
    # Look for first <p align="center">...</p> after the anchor
    m = re.search(r'<p[^>]*align="center"[^>]*>(.*?)</p>', slice_html, re.IGNORECASE | re.DOTALL)
    if not m:
        return fallback
    txt = clean_text(m.group(1))
    # Strip trailing period
    return txt.rstrip(".").strip() or fallback


def extract_paragraphs(slice_html: str, skip_first_centered: bool = True) -> list[str]:
    paras = []
    skipped = False
    for m in PARA_RE.finditer(slice_html):
        # If first centered paragraph is the title, skip it
        attrs = re.match(r'<p\b([^>]*)>', "<p" + slice_html[max(0, m.start()-1):m.start()+50])
        is_centered = 'align="center"' in (attrs.group(1).lower() if attrs else "") or "align=\"CENTER\"" in slice_html[max(0, m.start()-50):m.start()+50]
        txt = clean_text(m.group(1))
        if not txt or len(txt) < 4:
            continue
        if txt.lower() in {"main", "home", "keynotes by h.c. allen"}:
            continue
        if skip_first_centered and not skipped and is_centered:
            skipped = True
            continue
        # Skip Nash's "* * * * *" separator
        if re.fullmatch(r"[\*\s]+", txt):
            continue
        paras.append(txt)
    return paras


def parse_index(index_html: str) -> list[tuple[str, str, str]]:
    """Return list of (page_filename, anchor, displayed_name)."""
    out = []
    seen = set()
    for m in INDEX_LINK_RE.finditer(index_html):
        page, anchor, name = m.groups()
        # Skip nav links
        if page in {"intro.htm", "preface.htm", "grouping.htm", "theraindex.htm",
                    "index.htm", "allkeypr.htm"}:
            continue
        key = (page, anchor)
        if key in seen:
            continue
        seen.add(key)
        out.append((page, anchor, clean_text(name)))
    return out


def ingest(book_id: str, base_url: str, index_url: str, raw_dir: Path,
           out_filename: str, source_id: str) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    idx_html = fetch(index_url, raw_dir / "_index.htm")
    entries = parse_index(idx_html)
    print(f"[{book_id}] index: {len(entries)} remedy entries")

    # Group by page
    pages_needed = sorted({page for page, _, _ in entries})
    page_cache: dict[str, str] = {}
    for page in pages_needed:
        try:
            page_cache[page] = fetch(base_url + page, raw_dir / page)
        except Exception as e:
            print(f"  [!] page {page}: {e}", file=sys.stderr)
            page_cache[page] = ""

    records = []
    for page, anchor, name in entries:
        page_html = page_cache.get(page, "")
        if not page_html:
            continue
        slice_html = slice_remedy_html(page_html, anchor)
        if not slice_html:
            continue
        title = extract_centered_title(slice_html, name)
        paras = extract_paragraphs(slice_html, skip_first_centered=True)
        if not paras:
            continue
        rec = {
            "id": slugify(title),
            "names": {
                "primary": title,
                "latin": title,
            },
            "category": "homeopathic",
            "traditional": {
                "keynotes": [
                    {"text": p, "source_id": source_id} for p in paras
                ],
            },
            "provenance": {"sources": [source_id]},
        }
        records.append(rec)

    out = PROCESSED_DIR / out_filename
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Wrote {len(records)} records -> {out}")


CONFIGS = {
    "allen": {
        "base_url": "http://www.homeoint.org/books/allkeyn/",
        "index_url": "http://www.homeoint.org/books/allkeyn/index.htm",
        "raw_dir": ROOT / "data" / "raw" / "allen",
        "out": "allen_keynotes.json",
        "source_id": "allen-keynotes-1899",
    },
    "nash": {
        "base_url": "http://homeoint.org/books2/nashtherap/",
        "index_url": "http://homeoint.org/books2/nashtherap/index.htm",
        "raw_dir": ROOT / "data" / "raw" / "nash",
        "out": "nash.json",
        "source_id": "nash-leaders-1899",
    },
}


def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    todo = list(CONFIGS) if which == "all" else [which]
    for book in todo:
        cfg = CONFIGS[book]
        ingest(book, cfg["base_url"], cfg["index_url"], cfg["raw_dir"],
               cfg["out"], cfg["source_id"])


if __name__ == "__main__":
    main()
