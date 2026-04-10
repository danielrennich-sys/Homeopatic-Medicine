"""
Ingest Kent's Lectures on Homoeopathic Materia Medica (1905) from homeoint.org.

Public domain in the US (Kent d. 1916; pub. 1905).

Kent's lectures are flowing essay/lecture prose with no body-system section headers,
so we preserve each paragraph as a separate entry under traditional.keynotes,
keyed by source 'kent-lectures-1905'. Sub-headings (red bold inline labels) are
prepended to their paragraph text.
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
RAW_DIR = ROOT / "data" / "raw" / "kent"
PROCESSED_DIR = ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE = "http://homeoint.org/books3/kentmm/"
SOURCE_ID = "kent-lectures-1905"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE
HEADERS = {"User-Agent": "Mozilla/5.0 (homeopathy-db research/educational)"}

INDEX_RE = re.compile(
    r'<a href="([a-z0-9_\-]+)\.htm"[^>]*>\s*(\d+)\s*</a>\s*-?\s*([^<]+?)(?:<br|<)',
    re.IGNORECASE,
)

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
PARA_RE = re.compile(r"<p\b[^>]*>(.*?)</p>", re.IGNORECASE | re.DOTALL)


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


def extract_title(html: str, fallback: str) -> str:
    m = re.search(r'<a name="([^"]+)"\s*>([^<]+)</a>', html, re.IGNORECASE)
    if m:
        return clean_text(m.group(2))
    return fallback


def parse_remedy(slug: str, full_name: str, html: str) -> dict:
    title = extract_title(html, full_name)
    # Locate the main content region: between centered title and end of blockquote
    body_start = html.lower().find('a name="')
    if body_start == -1:
        body_start = 0
    body_end = html.lower().rfind("</blockquote>")
    if body_end == -1:
        body_end = len(html)
    body = html[body_start:body_end]

    paragraphs = []
    for m in PARA_RE.finditer(body):
        txt = clean_text(m.group(1))
        if not txt or len(txt) < 4:
            continue
        # Skip pure navigation crumbs
        if txt.lower() in {"main", "home"}:
            continue
        paragraphs.append(txt)

    record: dict = {
        "id": slugify(title or full_name),
        "names": {
            "primary": title or full_name,
            "latin": title or full_name,
        },
        "category": "homeopathic",
        "traditional": {
            "keynotes": [
                {"text": p, "source_id": SOURCE_ID} for p in paragraphs
            ],
        },
        "provenance": {"sources": [SOURCE_ID]},
    }
    return record


def main() -> None:
    idx_path = RAW_DIR / "kent_index.htm"
    idx_html = fetch(BASE + "index.htm", idx_path)
    entries = INDEX_RE.findall(idx_html)
    print(f"Index: {len(entries)} remedies")

    all_records = []
    for slug, num, name in entries:
        url = f"{BASE}{slug}.htm"
        cache = RAW_DIR / f"{slug}.htm"
        try:
            page = fetch(url, cache)
        except Exception as exc:
            print(f"  [!] {slug}: {exc}", file=sys.stderr)
            continue
        try:
            rec = parse_remedy(slug, clean_text(name), page)
            all_records.append(rec)
        except Exception as exc:
            print(f"  [!] parse {slug}: {exc}", file=sys.stderr)

    out = PROCESSED_DIR / "kent.json"
    out.write_text(json.dumps(all_records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(all_records)} records -> {out}")


if __name__ == "__main__":
    main()
