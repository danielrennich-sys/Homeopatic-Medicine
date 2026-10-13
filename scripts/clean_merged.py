"""
Post-merge identity cleanup for data/merged/.

Run after merge_remedies.py and tag_cell_salts.py, before the index builders:

    python scripts/merge_remedies.py
    python scripts/tag_cell_salts.py
    python scripts/clean_merged.py
    python scripts/build_search_index.py
    python scripts/build_passage_index.py

merge_remedies.py keys each source record on its own headline name, so it can
only see one record at a time. This script looks at the whole set and merges
records that are the same remedy under different ids:

  1. Curated ALIASES - genus-only entries that mean one species in homeopathy
     ("Bryonia" -> bryonia-alba), botanical synonyms (actaea-racemosa ->
     cimicifuga-racemosa), spelling variants.
  2. "-in" / "-ine" vs "-inum" (glonoine / glonoinum) when both exist.
  3. A genus-only record whose own headline names name a species
     ("AILANTHUS. (AILANTHUS GLANDULOSA.)" -> ailanthus-glandulosa).
  4. A genus-only plant record with exactly one species record under it
     (hydrastis -> hydrastis-canadensis). Chemical salts are never merged
     this way (camphora is not camphora-bromata).

It also gives every record a clean Title Case display name (the abbreviated
or ALL-CAPS headline each source used stays in synonyms, so name search still
finds it), moves the "Cell Salt #N - " prefix out of the display name (the
cell_salt_number field carries the badge), and rewrites _index.json.

Every merge is printed so the run can be reviewed.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from merge_remedies import CATEGORY_PRIORITY, display_name, merge_traditional, _fold  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
MERGED_DIR = Path(os.environ.get("HOMEO_MERGED_DIR", ROOT / "data" / "merged"))

# old id -> canonical id. The canonical id wins even if it has fewer sources.
ALIASES = {
    # genus-only entries that mean one species in the materia medica
    "aconitum": "aconitum-napellus",
    "agaricus": "agaricus-muscarius",
    "ailanthus": "ailanthus-glandulosa",
    "allium": "allium-sativum",
    "ambra": "ambra-grisea",
    "anacardium": "anacardium-orientale",
    "argentum": "argentum-metallicum",
    "arsenicum": "arsenicum-album",
    "arum": "arum-triphyllum",
    "asarum": "asarum-europaeum",
    "asclepias": "asclepias-tuberosa",
    "aurum": "aurum-metallicum",
    "baptisia": "baptisia-tinctoria",
    "berberis": "berberis-vulgaris",
    "bryonia": "bryonia-alba",
    "cactus": "cactus-grandiflorus",
    "caladium": "caladium-seguinum",
    "calcarea": "calcarea-carbonica",
    "calendula": "calendula-officinalis",
    "caulophyllum": "caulophyllum-thalictroides",
    "chelidonium": "chelidonium-majus",
    "chenopodium": "chenopodium-anthelminticum",
    "chimaphila": "chimaphila-umbellata",
    "china": "china-officinalis",
    "cimicifuga": "cimicifuga-racemosa",
    "cinnamomum": "cinnamomum-verum",
    "coffea": "coffea-cruda",
    "convallaria": "convallaria-majalis",
    "cuprum": "cuprum-metallicum",
    "dioscorea": "dioscorea-villosa",
    "equisetum": "equisetum-hyemale",
    "euphrasia": "euphrasia-officinalis",
    "ferrum": "ferrum-metallicum",
    "gelsemium": "gelsemium-sempervirens",
    "hamamelis": "hamamelis-virginiana",
    "helleborus": "helleborus-niger",
    "hepar": "hepar-sulphuris",
    "hypericum": "hypericum-perforatum",
    "inula": "inula-helenium",
    "ledum": "ledum-palustre",
    "lycopodium": "lycopodium-clavatum",
    "lycopus": "lycopus-virginicus",
    "manganum": "manganum-aceticum",
    "origanum": "origanum-majorana",
    "petroselinum": "petroselinum-sativum",
    "physostigma": "physostigma-venenosum",
    "phytolacca": "phytolacca-decandra",
    "plumbum": "plumbum-metallicum",
    "podophyllum": "podophyllum-peltatum",
    "rhododendron": "rhododendron-chrysanthum",
    "rumex": "rumex-crispus",
    "sambucus": "sambucus-nigra",
    "sanguinaria": "sanguinaria-canadensis",
    "scilla": "scilla-maritima",
    "scutellaria": "scutellaria-lateriflora",
    "secale": "secale-cornutum",
    "senecio": "senecio-aureus",
    "spigelia": "spigelia-anthelmia",
    "symphytum": "symphytum-officinale",
    "thuja": "thuja-occidentalis",
    "urtica": "urtica-urens",
    "valeriana": "valeriana-officinalis",
    "veratrum": "veratrum-album",
    "viscum": "viscum-album",
    "zincum": "zincum-metallicum",
    "zingiber": "zingiber-officinale",
    # species entries whose canonical homeopathic name is the genus
    "aloe-socotrina": "aloe",
    "aloe-vera": "aloe",
    "bufo-rana": "bufo",
    "cantharis-vesicatoria": "cantharis",
    "capsicum-annuum": "capsicum",
    "crataegus-oxyacantha": "crataegus",
    "lachesis-mutus": "lachesis",
    "lachesis-trigonocephalus": "lachesis",
    "lachesis-surukuku": "lachesis",
    "mercurius-vivus": "mercurius",
    "mercurius-solubilis": "mercurius",
    "pulsatilla-pratensis": "pulsatilla",
    "pulsatilla-nigricans": "pulsatilla",
    "pulsatilla-anemone": "pulsatilla",
    "rheum-palmatum": "rheum",
    "rheum-officinale": "rheum",
    "sarsaparilla-officinalis": "sarsaparilla",
    "sarsaparilla-smilax": "sarsaparilla",
    "sepia-officinalis": "sepia",
    # botanical synonyms / the same plant under two names
    "actaea-racemosa": "cimicifuga-racemosa",
    "macrotys": "cimicifuga-racemosa",
    "cereus-grandiflorus": "cactus-grandiflorus",
    "chamomilla-vulgaris": "chamomilla",
    "matricaria-chamomilla": "chamomilla",
    "matricaria": "chamomilla",
    "chrysanthemum-parthenium": "tanacetum-parthenium",
    "pyrethrum-parthenium": "tanacetum-parthenium",
    "datura-stramonium": "stramonium",
    "delphinium-staphisagria": "staphysagria",
    "staphisagria": "staphysagria",
    "gelsemium-nitidum": "gelsemium-sempervirens",
    "hydrophobinum": "lyssinum",
    "hydrophobin": "lyssinum",
    "hypericum-perfoliatum": "hypericum-perforatum",
    "millefolium": "achillea-millefolium",
    "myristica-fragrans": "nux-moschata",
    "nicotiana-tabacum": "tabacum",
    "petroselinum-crispum": "petroselinum-sativum",
    "psychotria-ipecacuanha": "ipecacuanha",
    "cephaelis-ipecacuanha": "ipecacuanha",
    "serenoa-repens": "sabal-serrulata",
    "serenoa-serrulata": "sabal-serrulata",
    "sabal": "sabal-serrulata",
    "silybum-marianum": "carduus-marianus",
    "strychnos-nux": "nux-vomica",
    "symphytum-officinalis": "symphytum-officinale",
    "thea": "camellia-sinensis",
    "thea-sinensis": "camellia-sinensis",
    "thea-chinensis": "camellia-sinensis",
    "urginea-scilla": "scilla-maritima",
    "urginea-maritima": "scilla-maritima",
    "vitex-agnus-castus": "agnus-castus",
    # different names for the same substance
    "aesculus": "aesculus-hippocastanum",
    "aloes": "aloe",
    "asa-f": "asafoetida",
    "ergot": "secale-cornutum",
    "ether": "aether",
    "ginseng-quinquefolium": "panax-quinquefolium",
    "panax": "panax-quinquefolium",
    "silica": "silicea",
    # two nosodes that some headings list together ("Vaccininum and Variolinum")
    "vaccininum-variolinum": "vaccininum",
    "variolinum-vaccininum": "variolinum",
    # acids under other names
    "nitri-acidum": "nitricum-acidum",
    "hydrochloricum-acidum": "muriaticum-acidum",
    "prussicum-acidum": "hydrocyanicum-acidum",
}

# Never auto-merge these genus-level ids by rule (their "species" are different substances)
ELEMENTS = {
    "aurum", "argentum", "cuprum", "ferrum", "plumbum", "stannum", "zincum", "kali", "natrum",
    "magnesia", "calcarea", "baryta", "ammonium", "mercurius", "arsenicum", "antimonium",
    "phosphorus", "sulphur", "carbo", "carboneum", "silicea", "alumina", "lithium", "manganum",
    "platinum", "palladium", "osmium", "iridium", "niccolum", "cobaltum", "cadmium", "bismuthum",
    "bismuth", "calcium", "magnesium", "sodium", "potassium", "iodum", "bromium", "chlorum",
    "selenium", "tellurium", "thallium", "uranium", "radium", "strontium", "titanium", "vanadium",
    "chromium", "aluminium", "hydrogen", "oxygen", "nitrogen", "carbon", "iron", "copper", "zinc",
    "lead", "tin", "silver", "gold", "mercury", "benzinum", "petroleum", "naphtha", "oleum",
    "spiritus", "aqua", "lac", "sal", "acidum", "amyl", "amylenum", "ethylum", "methylenum",
    "chininum", "atropinum", "morphinum", "strychninum", "caffeina", "camphora", "borax",
}
CHEMICAL_EPITHET = re.compile(
    r"(icum|ica|atum|ata|osum|osa|idum|ida|icus|atus|ate|ite|ide|metallicum|oxyd|oxid|sulph|sulf|"
    r"muriat|nitr|phosph|carbon|iod|brom|chlor|arsen|acet|citr|tartar|hydrogen|cyan|fluor|silic|"
    r"picr|salicyl|benzo|lact|boric|borac|chrom|ferro|magnet|hydrat|purum|pure|crud|praecip|"
    r"vivus|solubilis|corrosiv|dulcis|sublim|tosta|cruda|ustum|usta)$|^(hydrated|subnitrate|subgallate)"
)


def load_all() -> dict[str, dict]:
    recs = {}
    for p in sorted(MERGED_DIR.glob("*.json")):
        if p.name == "_index.json":
            continue
        d = json.loads(p.read_text(encoding="utf-8"))
        recs[d["id"]] = d
    return recs


def headline_tokens(rec: dict) -> set[str]:
    n = rec.get("names", {}) or {}
    words = set()
    for s in [n.get("primary", ""), n.get("latin", "")] + list(n.get("synonyms", [])) + list(n.get("common", [])):
        words.update(re.findall(r"[a-z]+", _fold(s or "").lower()))
    return words


def propose_merges(recs: dict[str, dict]) -> dict[str, tuple[str, str]]:
    """Return {donor_id: (canonical_id, reason)}."""
    plan: dict[str, tuple[str, str]] = {}
    ids = set(recs)
    curated_targets = set(ALIASES.values())

    # 1. curated
    for old, new in ALIASES.items():
        if old in ids and old != new:
            plan[old] = (new, "curated")

    # 2. -in / -ine -> -inum
    for i in sorted(ids):
        if "-" in i or i in plan:
            continue
        for suf, cut in (("ine", 3), ("in", 2)):
            if i.endswith(suf) and len(i) > cut + 3:
                target = i[:-cut] + "inum"
                if target in ids and target not in plan:
                    plan[i] = (target, "-inum spelling")
                break

    # 3./4. genus-only -> species
    by_genus: dict[str, list[str]] = {}
    for i in ids:
        if "-" in i:
            by_genus.setdefault(i.split("-", 1)[0], []).append(i)
    for g in sorted(ids):
        if "-" in g or g in plan or g in ELEMENTS or g in curated_targets:
            continue
        siblings = [s for s in by_genus.get(g, []) if s not in plan]
        if not siblings:
            continue
        epithets = {s: s.split("-", 1)[1] for s in siblings}
        # 3. own names mention exactly one sibling's species
        words = headline_tokens(recs[g])
        named = [s for s, ep in epithets.items() if ep in words and len(ep) >= 4]
        if len(named) == 1:
            plan[g] = (named[0], "names its species")
            continue
        # 4. exactly one plant species under this genus
        plants = [s for s, ep in epithets.items() if not CHEMICAL_EPITHET.search(ep) and len(ep) >= 4]
        if len(plants) == 1 and len(siblings) == 1:
            plan[g] = (plants[0], "single species")

    # resolve chains (a -> b -> c) and drop cycles
    def resolve(i: str, depth: int = 0) -> str:
        while i in plan and depth < 10:
            i = plan[i][0]
            depth += 1
        return i
    final = {}
    for donor, (target, reason) in plan.items():
        canon = resolve(target)
        if canon == donor:
            continue
        final[donor] = (canon, reason)
    return final


def merge_into(canon: dict, donor: dict) -> None:
    names = canon.setdefault("names", {})
    dn = donor.get("names", {}) or {}
    syns = names.setdefault("synonyms", [])
    seen = {s.lower() for s in syns} | {(names.get("primary") or "").lower()}
    for s in [dn.get("primary"), dn.get("latin")] + list(dn.get("synonyms", [])):
        if s and s.lower() not in seen:
            syns.append(s)
            seen.add(s.lower())
    common = names.setdefault("common", [])
    seenc = {c.lower() for c in common}
    for c in dn.get("common", []) or []:
        if c and c.lower() not in seenc:
            common.append(c)
            seenc.add(c.lower())
    cats = canon.setdefault("categories", [canon.get("category", "homeopathic")])
    for c in donor.get("categories", [donor.get("category")]) or []:
        if c and c not in cats:
            cats.append(c)
    cats.sort(key=lambda c: CATEGORY_PRIORITY.index(c) if c in CATEGORY_PRIORITY else 99)
    canon["category"] = cats[0]
    if donor.get("traditional"):
        merge_traditional(canon.setdefault("traditional", {}), donor["traditional"])
    if donor.get("evidence"):
        merge_traditional(canon.setdefault("evidence", {}), donor["evidence"])
    srcs = canon.setdefault("provenance", {}).setdefault("sources", [])
    for s in (donor.get("provenance", {}) or {}).get("sources", []):
        if s not in srcs:
            srcs.append(s)
    if donor.get("cell_salt_number") and not canon.get("cell_salt_number"):
        canon["cell_salt_number"] = donor["cell_salt_number"]


def fix_display_name(rec: dict) -> bool:
    """Title Case canonical display name; returns True if changed."""
    names = rec.setdefault("names", {})
    old_primary = names.get("primary") or ""
    old_latin = names.get("latin") or ""
    candidates = [n for n in [old_primary, old_latin] + list(names.get("synonyms", []))
                  if n and not n.lower().startswith("cell salt #")]
    primary = display_name(rec["id"], candidates)
    syns = names.setdefault("synonyms", [])
    seen = {s.lower() for s in syns} | {primary.lower()}
    for n in (old_primary, old_latin):
        if n and n.lower() not in seen:
            syns.append(n)
            seen.add(n.lower())
    names["synonyms"] = [s for s in syns if s.lower() != primary.lower()]
    changed = old_primary != primary
    names["primary"] = primary
    names["latin"] = primary
    return changed


def main() -> None:
    recs = load_all()
    print(f"Loaded {len(recs)} merged remedies from {MERGED_DIR}")
    plan = propose_merges(recs)

    # Apply merges (create the canonical from the donor if it does not exist yet)
    by_canon: dict[str, list[str]] = {}
    for donor, (canon, _) in plan.items():
        by_canon.setdefault(canon, []).append(donor)
    renamed = 0
    for canon_id, donors in sorted(by_canon.items()):
        if canon_id not in recs:
            first = donors.pop(0)
            recs[canon_id] = recs.pop(first)
            recs[canon_id]["id"] = canon_id
            (MERGED_DIR / f"{first}.json").unlink(missing_ok=True)
            print(f"  renamed  {first} -> {canon_id} ({plan[first][1]})")
            renamed += 1
        for d in donors:
            merge_into(recs[canon_id], recs[d])
            print(f"  merged   {d} -> {canon_id} ({plan[d][1]})")
            (MERGED_DIR / f"{d}.json").unlink(missing_ok=True)
            del recs[d]

    # Display names
    renamed_display = sum(fix_display_name(r) for r in recs.values())

    # Write everything back
    for i, r in recs.items():
        r["id"] = i
        (MERGED_DIR / f"{i}.json").write_text(json.dumps(r, indent=2, ensure_ascii=False), encoding="utf-8")

    idx_path = MERGED_DIR / "_index.json"
    index = json.loads(idx_path.read_text(encoding="utf-8")) if idx_path.exists() else {}
    if not isinstance(index, dict):
        index = {}
    index["total_canonical_remedies"] = len(recs)
    index["remedies"] = [{
        "id": i,
        "primary": r["names"]["primary"],
        "synonyms": r["names"].get("synonyms", []),
        "category": r.get("category", ""),
        "n_sources": len(r.get("provenance", {}).get("sources", [])),
        "sources": r.get("provenance", {}).get("sources", []),
        **({"cell_salt_number": r["cell_salt_number"]} if r.get("cell_salt_number") else {}),
    } for i, r in sorted(recs.items())]
    idx_path.write_text(json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nMerged {len(plan) - renamed} records into others, renamed {renamed}, "
          f"fixed {renamed_display} display names. {len(recs)} remedies remain.")


if __name__ == "__main__":
    main()
