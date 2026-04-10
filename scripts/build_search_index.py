"""
Build a compact search index from the merged remedy files for the web UI.

Output: data/search_index.json
  - Array of {id, primary, latin, common, synonyms, category, sources,
    n_sources, has_traditional, has_evidence, snippets}
  - snippets is a dict of field -> abbreviated text (first 300 chars)
    used for keyword matching and result previews.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MERGED_DIR = ROOT / "data" / "merged"
OUT = ROOT / "data" / "search_index.json"

WS_RE = re.compile(r"\s+")


def extract_text_snippets(block: dict, max_len: int = 400) -> dict[str, str]:
    """Recursively pull text from a traditional or evidence block."""
    out: dict[str, str] = {}
    for key, val in block.items():
        if isinstance(val, str) and len(val) > 10:
            out[key] = WS_RE.sub(" ", val)[:max_len]
        elif isinstance(val, list):
            parts = []
            for item in val:
                if isinstance(item, dict):
                    t = item.get("text", "")
                    if t:
                        parts.append(WS_RE.sub(" ", t)[:max_len])
                elif isinstance(item, str) and len(item) > 5:
                    parts.append(WS_RE.sub(" ", item)[:max_len])
            if parts:
                out[key] = " | ".join(parts)[:max_len * 2]
        elif isinstance(val, dict):
            sub = extract_text_snippets(val, max_len)
            for sk, sv in sub.items():
                out[f"{key}.{sk}"] = sv
    return out


def main() -> None:
    files = sorted(MERGED_DIR.glob("*.json"))
    files = [f for f in files if f.name != "_index.json"]
    print(f"Building search index from {len(files)} merged files")

    index = []
    for f in files:
        d = json.loads(f.read_text(encoding="utf-8"))
        names = d.get("names", {})
        trad = d.get("traditional", {})
        ev = d.get("evidence", {})

        # Build combined snippet text for searching
        snippets = {}
        if trad:
            snippets.update(extract_text_snippets(trad))
        if ev:
            for k, v in extract_text_snippets(ev).items():
                snippets[f"ev.{k}"] = v

        entry = {
            "id": d["id"],
            "primary": names.get("primary", ""),
            "latin": names.get("latin", ""),
            "common": names.get("common", []),
            "synonyms": names.get("synonyms", []),
            "category": d.get("category", ""),
            "sources": d.get("provenance", {}).get("sources", []),
            "n_sources": len(d.get("provenance", {}).get("sources", [])),
            "has_traditional": bool(trad),
            "has_evidence": bool(ev),
            "snippets": snippets,
        }
        index.append(entry)

    # Sort by number of sources descending
    index.sort(key=lambda r: (-r["n_sources"], r["primary"]))

    OUT.write_text(json.dumps(index, ensure_ascii=False), encoding="utf-8")
    size_mb = OUT.stat().st_size / 1024 / 1024
    print(f"Wrote {len(index)} entries -> {OUT} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
