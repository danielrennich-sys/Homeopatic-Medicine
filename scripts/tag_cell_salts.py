"""
Tag and merge the 12 Schuessler Cell Salts in the merged remedy database.

1. Merges duplicate files for the same cell salt into one canonical file
2. Adds "Cell Salt #N" to names and sets category metadata
3. Removes leftover duplicate files
4. Updates the _index.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MERGED_DIR = ROOT / "data" / "merged"

# The 12 Schuessler Cell Salts — canonical ID, number, display name,
# and list of file stems to merge into the canonical.
CELL_SALTS = [
    {
        "number": 1,
        "canonical_id": "calcarea-fluorica",
        "display": "Calcarea Fluorica",
        "latin": "Calcarea Fluorica",
        "common": ["Calc Fluor", "Calcium Fluoride", "Fluoride of Lime"],
        "merge_from": ["calcarea-fluorata", "calcarea-fluor"],
    },
    {
        "number": 2,
        "canonical_id": "calcarea-phosphorica",
        "display": "Calcarea Phosphorica",
        "latin": "Calcarea Phosphorica",
        "common": ["Calc Phos", "Calcium Phosphate", "Phosphate of Lime"],
        "merge_from": [],
    },
    {
        "number": 3,
        "canonical_id": "calcarea-sulphurica",
        "display": "Calcarea Sulphurica",
        "latin": "Calcarea Sulphurica",
        "common": ["Calc Sulph", "Calcium Sulphate", "Plaster of Paris"],
        "merge_from": [],
    },
    {
        "number": 4,
        "canonical_id": "ferrum-phosphoricum",
        "display": "Ferrum Phosphoricum",
        "latin": "Ferrum Phosphoricum",
        "common": ["Ferrum Phos", "Iron Phosphate", "Phosphate of Iron"],
        "merge_from": [],
    },
    {
        "number": 5,
        "canonical_id": "kali-muriaticum",
        "display": "Kali Muriaticum",
        "latin": "Kalium Muriaticum",
        "common": ["Kali Mur", "Potassium Chloride", "Chloride of Potash"],
        "merge_from": ["kalium-muriaticum"],
    },
    {
        "number": 6,
        "canonical_id": "kali-phosphoricum",
        "display": "Kali Phosphoricum",
        "latin": "Kalium Phosphoricum",
        "common": ["Kali Phos", "Potassium Phosphate", "Phosphate of Potash"],
        "merge_from": [],
    },
    {
        "number": 7,
        "canonical_id": "kali-sulphuricum",
        "display": "Kali Sulphuricum",
        "latin": "Kalium Sulphuricum",
        "common": ["Kali Sulph", "Potassium Sulphate", "Sulphate of Potash"],
        "merge_from": ["kalium-sulphuricum"],
        # Note: kali-sulphuratum is a different substance (Liver of Sulphur),
        # NOT cell salt #7 — do not merge it.
    },
    {
        "number": 8,
        "canonical_id": "magnesia-phosphorica",
        "display": "Magnesia Phosphorica",
        "latin": "Magnesia Phosphorica",
        "common": ["Mag Phos", "Magnesium Phosphate", "Phosphate of Magnesia"],
        "merge_from": [],
    },
    {
        "number": 9,
        "canonical_id": "natrum-muriaticum",
        "display": "Natrum Muriaticum",
        "latin": "Natrum Muriaticum",
        "common": ["Nat Mur", "Sodium Chloride", "Common Salt", "Table Salt"],
        "merge_from": [],
    },
    {
        "number": 10,
        "canonical_id": "natrum-phosphoricum",
        "display": "Natrum Phosphoricum",
        "latin": "Natrum Phosphoricum",
        "common": ["Nat Phos", "Sodium Phosphate", "Phosphate of Soda"],
        "merge_from": ["natrum-phos"],
    },
    {
        "number": 11,
        "canonical_id": "natrum-sulphuricum",
        "display": "Natrum Sulphuricum",
        "latin": "Natrum Sulphuricum",
        "common": ["Nat Sulph", "Sodium Sulphate", "Glauber's Salt"],
        "merge_from": ["natrum-sulphurosum"],
    },
    {
        "number": 12,
        "canonical_id": "silicea",
        "display": "Silicea",
        "latin": "Silicea Terra",
        "common": ["Silica", "Pure Flint", "Silicic Oxide", "Quartz"],
        "merge_from": ["silicea-terra", "silicea-pure"],
    },
]


def deep_merge_traditional(target: dict, source: dict) -> None:
    """Merge source traditional block into target, combining lists
    and not overwriting existing text fields."""
    for key, val in source.items():
        if key not in target:
            target[key] = val
        elif isinstance(val, list) and isinstance(target[key], list):
            # Append items that aren't duplicates (check by text content)
            existing_texts = set()
            for item in target[key]:
                if isinstance(item, dict) and "text" in item:
                    existing_texts.add(item["text"][:200])
                elif isinstance(item, str):
                    existing_texts.add(item[:200])
            for item in val:
                if isinstance(item, dict) and "text" in item:
                    if item["text"][:200] not in existing_texts:
                        target[key].append(item)
                elif isinstance(item, str):
                    if item[:200] not in existing_texts:
                        target[key].append(item)
        elif isinstance(val, dict) and isinstance(target[key], dict):
            deep_merge_traditional(target[key], val)
        elif isinstance(val, str) and isinstance(target[key], str):
            # Append if substantially different and both non-empty
            if len(val) > 20 and val[:100] not in target[key][:500]:
                target[key] = target[key] + "\n\n" + val


def merge_remedy_files(canonical_path: Path, donor_paths: list[Path]) -> int:
    """Merge donor files into canonical file. Returns number of donors merged."""
    if not canonical_path.exists():
        print(f"  [!] Canonical file missing: {canonical_path.name}")
        return 0

    canonical = json.loads(canonical_path.read_text(encoding="utf-8"))
    merged_count = 0

    for donor_path in donor_paths:
        if not donor_path.exists():
            continue
        donor = json.loads(donor_path.read_text(encoding="utf-8"))
        print(f"    Merging {donor_path.name} -> {canonical_path.name}")

        # Merge sources
        existing_sources = set(canonical.get("provenance", {}).get("sources", []))
        for src in donor.get("provenance", {}).get("sources", []):
            if src not in existing_sources:
                canonical.setdefault("provenance", {}).setdefault("sources", []).append(src)
                existing_sources.add(src)

        # Merge synonyms
        existing_syns = set(canonical.get("names", {}).get("synonyms", []))
        for syn in donor.get("names", {}).get("synonyms", []):
            if syn not in existing_syns:
                canonical.setdefault("names", {}).setdefault("synonyms", []).append(syn)
                existing_syns.add(syn)

        # Add donor's primary name as synonym if different
        donor_primary = donor.get("names", {}).get("primary", "")
        if donor_primary and donor_primary not in existing_syns:
            canonical_primary = canonical.get("names", {}).get("primary", "")
            if donor_primary.lower() != canonical_primary.lower():
                canonical.setdefault("names", {}).setdefault("synonyms", []).append(donor_primary)

        # Merge traditional blocks
        donor_trad = donor.get("traditional", {})
        if donor_trad:
            canonical.setdefault("traditional", {})
            deep_merge_traditional(canonical["traditional"], donor_trad)

        # Merge evidence blocks
        donor_ev = donor.get("evidence", {})
        if donor_ev:
            canonical.setdefault("evidence", {})
            deep_merge_traditional(canonical["evidence"], donor_ev)

        merged_count += 1

    # Write back
    canonical_path.write_text(
        json.dumps(canonical, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return merged_count


def main() -> None:
    print("=== Tagging and Merging Schuessler Cell Salts ===\n")

    removed_files = []

    for cs in CELL_SALTS:
        num = cs["number"]
        cid = cs["canonical_id"]
        canonical_path = MERGED_DIR / f"{cid}.json"

        print(f"Cell Salt #{num}: {cs['display']}")

        # Step 1: Merge duplicates into canonical
        if cs["merge_from"]:
            donor_paths = [MERGED_DIR / f"{stem}.json" for stem in cs["merge_from"]]
            n = merge_remedy_files(canonical_path, donor_paths)
            if n:
                print(f"    Merged {n} file(s)")
            # Remove donor files
            for stem in cs["merge_from"]:
                p = MERGED_DIR / f"{stem}.json"
                if p.exists():
                    p.unlink()
                    removed_files.append(p.name)
                    print(f"    Removed {p.name}")

        # Step 2: Tag as cell salt
        if not canonical_path.exists():
            print(f"  [!] MISSING: {canonical_path.name}")
            continue

        data = json.loads(canonical_path.read_text(encoding="utf-8"))

        # Update names
        names = data.setdefault("names", {})
        names["primary"] = f"Cell Salt #{num} - {cs['display']}"
        names["latin"] = cs["latin"]

        # Merge common names
        existing_common = set(names.get("common", []))
        for c in cs["common"]:
            if c not in existing_common:
                names.setdefault("common", []).append(c)
                existing_common.add(c)

        # Add "Cell Salt" to synonyms if not there
        syns = set(names.get("synonyms", []))
        for alias in [f"Cell Salt #{num}", f"Schuessler Salt #{num}",
                      f"Tissue Salt #{num}", cs["display"]]:
            if alias not in syns:
                names.setdefault("synonyms", []).append(alias)
                syns.add(alias)

        # Set cell_salt metadata
        data["cell_salt_number"] = num
        data["category"] = "homeopathic"

        # Write back
        canonical_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        src_count = len(data.get("provenance", {}).get("sources", []))
        print(f"    Tagged: {names['primary']} ({src_count} sources)")

    # Step 3: Update _index.json
    index_path = MERGED_DIR / "_index.json"
    if index_path.exists():
        index = json.loads(index_path.read_text(encoding="utf-8"))
        # Remove entries for deleted files
        removed_ids = set()
        for cs in CELL_SALTS:
            for stem in cs.get("merge_from", []):
                removed_ids.add(stem)
        if isinstance(index, list):
            index = [e for e in index if e.get("id") not in removed_ids]
            index_path.write_text(
                json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8"
            )

    print(f"\nDone! Removed {len(removed_files)} duplicate files.")
    print("Run `python scripts/build_search_index.py` to rebuild the search index.")


if __name__ == "__main__":
    main()
