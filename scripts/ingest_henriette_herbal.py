"""
Generic ingester for eclectic/herbal materia medica books on
henriettes-herb.com (Henriette Kress's digitization, public-domain texts):

  - King's American Dispensatory (1898) — Felter & Lloyd
  - Ellingwood's American Materia Medica (1919)
  - Felter's Eclectic Materia Medica (1922)

All three live under /eclectic/<book>/index.html with one HTML page per
botanical entry. The site is UTF-8, modern Drupal markup; each entry has:
  <h1 class="page-title">Latin name.—Common name.</h1>
  ...
  <div class="field field-name-body ...">
     <p>...paragraphs...</p>
  </div>

Categories of these books: herbal (eclectic), not homeopathic.
"""
from __future__ import annotations

import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
from html import unescape
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CTX = ssl.create_default_context()
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
PARA_RE = re.compile(r"<p\b[^>]*>(.*?)</p>", re.IGNORECASE | re.DOTALL)
TITLE_RE = re.compile(
    r'<h1\s+class="page-title"[^>]*>(.*?)</h1>', re.IGNORECASE | re.DOTALL
)
BODY_RE = re.compile(
    r'class="field field-name-body[^"]*"[^>]*>(.*?)</article>',
    re.DOTALL | re.IGNORECASE,
)

# Pages that aren't remedy entries
SKIP_BASENAMES = {
    "index", "intro", "preface", "acknowl", "solvents", "index-other",
    "tidbits", "abbrev", "pics", "copyright",
}
SKIP_PREFIXES = ("illustration-",)


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
        time.sleep(0.4)
    if not raw:
        return None
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


def parse_title(raw_title: str) -> tuple[str, str]:
    """Split 'Abies Canadensis.—Hemlock Spruce.' into (latin, common)."""
    txt = clean_text(raw_title)
    # split on em-dash or en-dash or hyphen between phrases
    m = re.split(r"\s*[\u2014\u2013\-]\s*", txt, maxsplit=1)
    if len(m) == 2:
        latin = m[0].rstrip(".").strip()
        common = m[1].rstrip(".").strip()
    else:
        latin = txt.rstrip(".").strip()
        common = ""
    return latin, common


def parse_remedy(html: str, book_id: str, source_id: str) -> dict | None:
    tm = TITLE_RE.search(html)
    if not tm:
        return None
    latin, common = parse_title(tm.group(1))
    if not latin:
        return None

    bm = BODY_RE.search(html)
    if not bm:
        return None
    body_html = bm.group(1)

    paras: list[str] = []
    for m in PARA_RE.finditer(body_html):
        txt = clean_text(m.group(1))
        if not txt or len(txt) < 5:
            continue
        # Drop "Related entries:" navigation paragraph
        if txt.lower().startswith("related entries"):
            continue
        paras.append(txt)
    if not paras:
        return None

    record: dict = {
        "id": slugify(latin),
        "names": {
            "primary": latin,
            "latin": latin,
            "common": [common] if common else [],
        },
        "category": "herbal",
        "traditional": {
            "indications": [{"text": p, "source_id": source_id} for p in paras],
        },
        "provenance": {"sources": [source_id]},
    }
    return record


def parse_links(html: str, book_path: str) -> list[tuple[str, str]]:
    """Return [(slug, name), ...] for any link inside the book namespace."""
    pat = re.compile(
        rf'<a href="{re.escape(book_path)}([a-z0-9_\-]+)\.html"[^>]*>([^<]+)</a>',
        re.IGNORECASE,
    )
    out = []
    seen = set()
    for m in pat.finditer(html):
        slug = m.group(1)
        if slug in seen:
            continue
        seen.add(slug)
        out.append((slug, clean_text(m.group(2))))
    return out


# Slug prefixes that indicate navigation/category pages, not remedy entries
NAV_PREFIXES_DEFAULT = ()
NAV_PREFIXES_ELLINGWOOD = ("group", "g1-", "g2-", "g3-", "g4-", "g5-",
                            "g6-", "g7-", "g8-", "g9-", "g10-", "gg")


def is_nav_slug(slug: str, nav_prefixes: tuple[str, ...]) -> bool:
    if slug in SKIP_BASENAMES:
        return True
    if any(slug.startswith(p) for p in SKIP_PREFIXES):
        return True
    if any(slug.startswith(p) for p in nav_prefixes):
        return True
    return False


def discover_remedy_slugs(base_url: str, book_path: str, raw_dir: Path,
                          nav_prefixes: tuple[str, ...]) -> list[tuple[str, str]]:
    """BFS through index + nav pages, return list of (slug, displayed_name)
    for actual remedy entries only."""
    visited: set[str] = set()
    remedies: dict[str, str] = {}
    queue: list[str] = ["index"]
    while queue:
        slug = queue.pop(0)
        if slug in visited:
            continue
        visited.add(slug)
        url = base_url + slug + ".html"
        cache = raw_dir / f"{slug}.html"
        try:
            page = fetch(url, cache, allow_404=True)
        except Exception as exc:
            print(f"  [discover !] {slug}: {exc}", file=sys.stderr)
            continue
        if not page:
            continue
        for child_slug, child_name in parse_links(page, book_path):
            if child_slug in visited:
                continue
            if is_nav_slug(child_slug, nav_prefixes):
                queue.append(child_slug)
            else:
                if child_slug not in remedies:
                    remedies[child_slug] = child_name
    return sorted(remedies.items())


def ingest(book_id: str, base_url: str, book_path: str, raw_dir: Path,
           out_filename: str, source_id: str,
           nav_prefixes: tuple[str, ...] = NAV_PREFIXES_DEFAULT) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    entries = discover_remedy_slugs(base_url, book_path, raw_dir, nav_prefixes)
    print(f"[{book_id}] {len(entries)} remedy entries discovered")

    records = []
    for slug, name in entries:
        url = base_url + slug + ".html"
        cache = raw_dir / f"{slug}.html"
        try:
            page = fetch(url, cache, allow_404=True)
        except Exception as exc:
            print(f"  [!] {slug}: {exc}", file=sys.stderr)
            continue
        if not page:
            continue
        try:
            rec = parse_remedy(page, book_id, source_id)
            if rec:
                records.append(rec)
        except Exception as exc:
            print(f"  [!] parse {slug}: {exc}", file=sys.stderr)

    out = PROCESSED_DIR / out_filename
    out.write_text(json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  Wrote {len(records)} records -> {out}")


CONFIGS = {
    "kings": {
        "base_url": "https://www.henriettes-herb.com/eclectic/kings/",
        "book_path": "/eclectic/kings/",
        "raw_dir": ROOT / "data" / "raw" / "kings",
        "out": "kings_dispensatory.json",
        "source_id": "kings-american-1898",
        "nav_prefixes": NAV_PREFIXES_DEFAULT,
    },
    "ellingwood": {
        "base_url": "https://www.henriettes-herb.com/eclectic/ellingwood/",
        "book_path": "/eclectic/ellingwood/",
        "raw_dir": ROOT / "data" / "raw" / "ellingwood",
        "out": "ellingwood.json",
        "source_id": "ellingwood-1919",
        "nav_prefixes": NAV_PREFIXES_ELLINGWOOD,
    },
    "felter": {
        "base_url": "https://www.henriettes-herb.com/eclectic/felter/",
        "book_path": "/eclectic/felter/",
        "raw_dir": ROOT / "data" / "raw" / "felter",
        "out": "felter_eclectic.json",
        "source_id": "felter-eclectic-1922",
        "nav_prefixes": NAV_PREFIXES_DEFAULT,
    },
}


def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    todo = list(CONFIGS) if which == "all" else [which]
    for book in todo:
        cfg = CONFIGS[book]
        ingest(book, cfg["base_url"], cfg["book_path"], cfg["raw_dir"],
               cfg["out"], cfg["source_id"], cfg["nav_prefixes"])


if __name__ == "__main__":
    main()
