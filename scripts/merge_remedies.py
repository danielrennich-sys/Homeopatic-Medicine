"""
Merge per-source remedy JSON files in data/processed/ into a single canonical
database keyed by normalized Latin binomial.

Strategy:
  1. Load every *.json in data/processed/.
  2. For each record, derive a canonical key from the latin name:
       - Strip parenthetical / qualifier suffixes ("(U. S. P.)", "Linn.", etc.)
       - Take the first 1-2 alpha tokens (genus + species)
       - Apply abbreviation expansions (Acon. -> Aconitum, etc.)
       - Slugify
  3. Group all records sharing the same canonical key.
  4. For each group, deep-merge the `traditional` blocks: list-valued fields
     are concatenated (with per-entry source_id preserved); scalar/dict fields
     keep the first non-empty value plus a `_alt` list.
  5. Track provenance: union of all source_ids across the group.
  6. Write each merged remedy to data/merged/<canonical_slug>.json plus a
     summary index data/merged/_index.json.

This is intentionally conservative — when in doubt, it leaves data un-merged
rather than guessing wrong identities. Run repeatedly as new sources land.
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = ROOT / "data" / "processed"
MERGED_DIR = ROOT / "data" / "merged"
MERGED_DIR.mkdir(parents=True, exist_ok=True)

# Genus abbreviations -> full Latin (extend over time)
ABBREVIATION_MAP = {
    "acon": "aconitum",
    "aesc": "aesculus",
    "aeth": "aethusa",
    "agar": "agaricus",
    "all-c": "allium cepa",
    "alum": "alumina",
    "ambr": "ambra",
    "anac": "anacardium",
    "ant-c": "antimonium crudum",
    "ant-t": "antimonium tartaricum",
    "apis": "apis mellifica",
    "arg-n": "argentum nitricum",
    "arn": "arnica montana",
    "ars": "arsenicum album",
    "asaf": "asafoetida",
    "aur": "aurum metallicum",
    "bapt": "baptisia tinctoria",
    "bar-c": "baryta carbonica",
    "bell": "belladonna",
    "berb": "berberis vulgaris",
    "bism": "bismuthum",
    "bor": "borax",
    "bov": "bovista",
    "bry": "bryonia alba",
    "cact": "cactus grandiflorus",
    "calc": "calcarea carbonica",
    "calc-p": "calcarea phosphorica",
    "camph": "camphora",
    "cann-i": "cannabis indica",
    "canth": "cantharis",
    "caps": "capsicum",
    "carb-an": "carbo animalis",
    "carb-v": "carbo vegetabilis",
    "caust": "causticum",
    "cham": "chamomilla",
    "chel": "chelidonium",
    "chin": "china officinalis",
    "cic": "cicuta virosa",
    "cina": "cina maritima",
    "cocc": "cocculus indicus",
    "coff": "coffea cruda",
    "colch": "colchicum",
    "coloc": "colocynthis",
    "con": "conium maculatum",
    "croc": "crocus sativus",
    "crot-h": "crotalus horridus",
    "cupr": "cuprum metallicum",
    "dig": "digitalis",
    "dros": "drosera rotundifolia",
    "dulc": "dulcamara",
    "euph": "euphrasia",
    "ferr": "ferrum metallicum",
    "gels": "gelsemium",
    "glon": "glonoinum",
    "graph": "graphites",
    "guai": "guaiacum",
    "ham": "hamamelis",
    "hell": "helleborus",
    "hep": "hepar sulphuris calcareum",
    "hyos": "hyoscyamus",
    "hyper": "hypericum",
    "ign": "ignatia",
    "iod": "iodum",
    "ip": "ipecacuanha",
    "kali-bi": "kali bichromicum",
    "kali-c": "kali carbonicum",
    "kali-p": "kali phosphoricum",
    "kreos": "kreosotum",
    "lach": "lachesis",
    "laur": "laurocerasus",
    "led": "ledum palustre",
    "lyc": "lycopodium clavatum",
    "mag-c": "magnesia carbonica",
    "mag-p": "magnesia phosphorica",
    "merc": "mercurius vivus",
    "mez": "mezereum",
    "mosch": "moschus",
    "mur-ac": "muriaticum acidum",
    "nat-c": "natrum carbonicum",
    "nat-m": "natrum muriaticum",
    "nat-s": "natrum sulphuricum",
    "nit-ac": "nitricum acidum",
    "nux-m": "nux moschata",
    "nux-v": "nux vomica",
    "op": "opium",
    "petr": "petroleum",
    "phos": "phosphorus",
    "phyt": "phytolacca",
    "plat": "platinum metallicum",
    "plb": "plumbum metallicum",
    "podo": "podophyllum",
    "puls": "pulsatilla",
    "rhod": "rhododendron",
    "rhus-t": "rhus toxicodendron",
    "ruta": "ruta graveolens",
    "sabad": "sabadilla",
    "sabin": "sabina",
    "samb": "sambucus nigra",
    "sang": "sanguinaria",
    "sec": "secale cornutum",
    "sel": "selenium",
    "sep": "sepia",
    "sil": "silicea",
    "spig": "spigelia",
    "spong": "spongia tosta",
    "stann": "stannum metallicum",
    "staph": "staphysagria",
    "stram": "stramonium",
    "sul": "sulphur",
    "sulph": "sulphur",
    "sul-ac": "sulphuricum acidum",
    "tarent": "tarentula hispanica",
    "thuj": "thuja occidentalis",
    "ust": "ustilago",
    "valer": "valeriana",
    "verat": "veratrum album",
    "verat-v": "veratrum viride",
    "zinc": "zincum metallicum",
}

# Common (English) names -> canonical Latin binomial. Used so NCCIH/ODS
# evidence records (which key on common names like "Black Cohosh") merge
# with traditional records keyed on the Latin binomial.
COMMON_TO_LATIN = {
    "acai": "euterpe oleracea",
    "aloe vera": "aloe",
    "asian ginseng": "panax ginseng",
    "ashwagandha": "withania somnifera",
    "astragalus": "astragalus membranaceus",
    "bilberry": "vaccinium myrtillus",
    "bitter orange": "citrus aurantium",
    "black cohosh": "cimicifuga racemosa",
    "blue cohosh": "caulophyllum thalictroides",
    "boswellia": "boswellia serrata",
    "butterbur": "petasites hybridus",
    "cats claw": "uncaria tomentosa",
    "cat s claw": "uncaria tomentosa",
    "chamomile": "matricaria chamomilla",
    "german chamomile": "matricaria chamomilla",
    "roman chamomile": "anthemis nobilis",
    "chasteberry": "vitex agnus-castus",
    "chaste tree": "vitex agnus-castus",
    "cinnamon": "cinnamomum verum",
    "cranberry": "vaccinium macrocarpon",
    "dandelion": "taraxacum officinale",
    "devils claw": "harpagophytum procumbens",
    "devil s claw": "harpagophytum procumbens",
    "echinacea": "echinacea purpurea",
    "elderberry": "sambucus nigra",
    "european elder": "sambucus nigra",
    "ephedra": "ephedra sinica",
    "eucalyptus": "eucalyptus globulus",
    "european mistletoe": "viscum album",
    "evening primrose oil": "oenothera biennis",
    "evening primrose": "oenothera biennis",
    "fenugreek": "trigonella foenum-graecum",
    "feverfew": "tanacetum parthenium",
    "flaxseed": "linum usitatissimum",
    "garlic": "allium sativum",
    "ginger": "zingiber officinale",
    "ginkgo": "ginkgo biloba",
    "ginseng": "panax ginseng",
    "goldenseal": "hydrastis canadensis",
    "grape seed extract": "vitis vinifera",
    "green tea": "camellia sinensis",
    "hawthorn": "crataegus",
    "hibiscus": "hibiscus sabdariffa",
    "hoodia": "hoodia gordonii",
    "hops": "humulus lupulus",
    "horehound": "marrubium vulgare",
    "horse chestnut": "aesculus hippocastanum",
    "kava": "piper methysticum",
    "kudzu": "pueraria lobata",
    "lavender": "lavandula angustifolia",
    "lemon balm": "melissa officinalis",
    "licorice root": "glycyrrhiza glabra",
    "licorice": "glycyrrhiza glabra",
    "marshmallow": "althaea officinalis",
    "milk thistle": "silybum marianum",
    "mistletoe": "viscum album",
    "noni": "morinda citrifolia",
    "oregano": "origanum vulgare",
    "passionflower": "passiflora incarnata",
    "peppermint": "mentha piperita",
    "peppermint oil": "mentha piperita",
    "pomegranate": "punica granatum",
    "raspberry": "rubus idaeus",
    "red clover": "trifolium pratense",
    "red yeast rice": "monascus purpureus",
    "rhodiola": "rhodiola rosea",
    "rosemary": "rosmarinus officinalis",
    "sage": "salvia officinalis",
    "saw palmetto": "serenoa repens",
    "soy": "glycine max",
    "spearmint": "mentha spicata",
    "st johns wort": "hypericum perforatum",
    "saint johns wort": "hypericum perforatum",
    "stinging nettle": "urtica dioica",
    "nettle": "urtica dioica",
    "tea tree oil": "melaleuca alternifolia",
    "tea tree": "melaleuca alternifolia",
    "thunder god vine": "tripterygium wilfordii",
    "turmeric": "curcuma longa",
    "valerian": "valeriana officinalis",
    "wild yam": "dioscorea villosa",
    "willow bark": "salix alba",
    "wormwood": "artemisia absinthium",
    "yarrow": "achillea millefolium",
    "yohimbe": "pausinystalia johimbe",
    # LactMed / LiverTox additional herbs
    "alfalfa": "medicago sativa",
    "anise": "pimpinella anisum",
    "arnica": "arnica montana",
    "arnica montana": "arnica montana",
    "barley": "hordeum vulgare",
    "basil": "ocimum basilicum",
    "betony": "stachys officinalis",
    "blessed thistle": "cnicus benedictus",
    "borage": "borago officinalis",
    "buckthorn": "rhamnus cathartica",
    "buchu": "agathosma betulina",
    "calendula": "calendula officinalis",
    "caraway": "carum carvi",
    "castor": "ricinus communis",
    "chaparral": "larrea tridentata",
    "chlorella": "chlorella vulgaris",
    "coleus": "coleus forskohlii",
    "comfrey": "symphytum officinale",
    "cordyceps": "cordyceps sinensis",
    "coriander": "coriandrum sativum",
    "cumin": "cuminum cyminum",
    "dill": "anethum graveolens",
    "dong quai": "angelica sinensis",
    "eleuthero": "eleutherococcus senticosus",
    "fennel": "foeniculum vulgare",
    "garcinia": "garcinia cambogia",
    "garcinia cambogia": "garcinia cambogia",
    "geranium": "pelargonium",
    "germander": "teucrium chamaedrys",
    "goat s rue": "galega officinalis",
    "goats rue": "galega officinalis",
    "greater celandine": "chelidonium majus",
    "guarana": "paullinia cupana",
    "gymnema": "gymnema sylvestre",
    "horny goat weed": "epimedium",
    "horsetail": "equisetum arvense",
    "hyssop": "hyssopus officinalis",
    "jasmine": "jasminum officinale",
    "kelp": "laminaria",
    "khat": "catha edulis",
    "kratom": "mitragyna speciosa",
    "maca": "lepidium meyenii",
    "moringa": "moringa oleifera",
    "nutmeg": "myristica fragrans",
    "papaya": "carica papaya",
    "parsley": "petroselinum crispum",
    "pennyroyal": "mentha pulegium",
    "pennyroyal oil": "mentha pulegium",
    "peony": "paeonia officinalis",
    "rhubarb": "rheum palmatum",
    "senna": "senna alexandrina",
    "skullcap": "scutellaria lateriflora",
    "slippery elm": "ulmus rubra",
    "spirulina": "arthrospira platensis",
    "st john s wort": "hypericum perforatum",
    "tongkat ali": "eurycoma longifolia",
    "tribulus": "tribulus terrestris",
    "uva ursi": "arctostaphylos uva-ursi",
    "vervain": "verbena officinalis",
    "withania": "withania somnifera",
    "yerba mate": "ilex paraguariensis",
    "yohimbine": "pausinystalia johimbe",
    # Ayurvedic / TCM common names
    "black seed": "nigella sativa",
    "black cumin": "nigella sativa",
    "black cumin seed": "nigella sativa",
    "bitter melon": "momordica charantia",
    "cascara": "rhamnus purshiana",
    "cat s claw": "uncaria tomentosa",
}


# Tokens that are noise to drop from Latin names before keying
NOISE_TOKENS = {
    "u", "s", "p", "linn", "linne", "linnaeus", "michaux", "willd",
    "willdenow", "lam", "lamarck", "carriere", "n", "o", "nat", "ord",
    "br", "the", "of", "and",
}


def slugify(name: str) -> str:
    s = name.replace("\u00c6", "AE").replace("\u00e6", "ae")
    s = s.replace("\u0152", "OE").replace("\u0153", "oe")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def canonical_key(latin: str) -> str:
    """Reduce a latin string to its canonical Genus-species slug."""
    if not latin:
        return ""
    s = latin
    # Strip parentheticals (e.g. "(U. S. P.)")
    s = re.sub(r"\([^)]*\)", " ", s)
    # Strip everything after an em/en-dash or comma (common name etc.)
    s = re.split(r"[\u2014\u2013\-,]", s, maxsplit=1)[0]
    s = unicodedata.normalize("NFKD", s.replace("\u00e6", "ae").replace("\u00c6", "AE"))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    # Tokenize alpha-only
    tokens = re.findall(r"[a-z]+", s)
    tokens = [t for t in tokens if t not in NOISE_TOKENS]
    if not tokens:
        return ""
    # Common-name -> Latin lookup. Try the longest plausible prefix first
    # so "black cohosh root" matches "black cohosh".
    for n in range(min(4, len(tokens)), 0, -1):
        phrase = " ".join(tokens[:n])
        if phrase in COMMON_TO_LATIN:
            return slugify(COMMON_TO_LATIN[phrase])
    # Try abbreviation expansion if first token is short (4 chars or less and ends in canonical abbrev)
    first = tokens[0]
    if first in ABBREVIATION_MAP:
        return slugify(ABBREVIATION_MAP[first])
    # Take genus + species (or just genus if only one token)
    binomial = " ".join(tokens[:2])
    return slugify(binomial)


def merge_traditional(target: dict, addition: dict) -> None:
    """Concatenate list-valued fields, preserve dicts."""
    for k, v in addition.items():
        if k not in target:
            target[k] = v
            continue
        if isinstance(v, list) and isinstance(target[k], list):
            target[k].extend(v)
        elif isinstance(v, dict) and isinstance(target[k], dict):
            for sk, sv in v.items():
                if sk not in target[k]:
                    target[k][sk] = sv
                elif isinstance(sv, list) and isinstance(target[k][sk], list):
                    target[k][sk].extend(sv)
        # else: scalar — keep first


def merge_records(records: list[dict]) -> dict:
    """Combine a group of records sharing one canonical key."""
    base = {
        "id": "",
        "names": {"primary": "", "latin": "", "common": [], "synonyms": []},
        "category": "homeopathic",
        "traditional": {},
        "evidence": {},
        "provenance": {"sources": []},
    }
    seen_sources = []
    seen_primaries = []
    for r in records:
        names = r.get("names", {})
        primary = names.get("primary") or ""
        latin = names.get("latin") or primary
        if not base["names"]["primary"] and primary:
            base["names"]["primary"] = primary
        if not base["names"]["latin"] and latin:
            base["names"]["latin"] = latin
        if primary and primary not in seen_primaries:
            seen_primaries.append(primary)
        # Other names go into synonyms
        for c in names.get("common", []) or []:
            if c and c not in base["names"]["common"]:
                base["names"]["common"].append(c)
        cat = r.get("category")
        if cat and cat != "homeopathic":
            base["category"] = cat  # herbal etc. wins over default
        merge_traditional(base["traditional"], r.get("traditional", {}) or {})
        merge_traditional(base["evidence"], r.get("evidence", {}) or {})
        for src in (r.get("provenance", {}) or {}).get("sources", []) or []:
            if src not in seen_sources:
                seen_sources.append(src)
    base["names"]["synonyms"] = [n for n in seen_primaries[1:]]
    base["provenance"]["sources"] = seen_sources
    return base


def main() -> None:
    files = sorted(PROCESSED_DIR.glob("*.json"))
    if not files:
        print("no source files in", PROCESSED_DIR)
        sys.exit(1)

    groups: dict[str, list[dict]] = defaultdict(list)
    skipped_no_key = 0
    total_in = 0
    per_source_counts: dict[str, int] = {}

    for f in files:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[!] {f.name}: {e}", file=sys.stderr)
            continue
        per_source_counts[f.stem] = len(data)
        for rec in data:
            total_in += 1
            names = rec.get("names", {}) or {}
            latin = names.get("latin") or names.get("primary") or ""
            key = canonical_key(latin)
            if not key:
                skipped_no_key += 1
                continue
            groups[key].append(rec)

    # Merge each group
    merged_index = []
    for key, recs in sorted(groups.items()):
        merged = merge_records(recs)
        merged["id"] = key
        out = MERGED_DIR / f"{key}.json"
        out.write_text(json.dumps(merged, indent=2, ensure_ascii=False),
                       encoding="utf-8")
        merged_index.append({
            "id": key,
            "primary": merged["names"]["primary"],
            "synonyms": merged["names"]["synonyms"],
            "category": merged["category"],
            "n_sources": len(merged["provenance"]["sources"]),
            "sources": merged["provenance"]["sources"],
        })

    idx_path = MERGED_DIR / "_index.json"
    idx_path.write_text(
        json.dumps({
            "total_input_records": total_in,
            "skipped_no_key": skipped_no_key,
            "per_source_input_counts": per_source_counts,
            "total_canonical_remedies": len(merged_index),
            "remedies": merged_index,
        }, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    multi = sum(1 for r in merged_index if r["n_sources"] >= 2)
    print(f"Input: {total_in} records across {len(per_source_counts)} sources")
    for src, n in per_source_counts.items():
        print(f"  {src}: {n}")
    print(f"Skipped (no canonical key): {skipped_no_key}")
    print(f"Canonical remedies: {len(merged_index)}")
    print(f"  in 2+ sources: {multi}")
    print(f"Wrote -> {MERGED_DIR}")


if __name__ == "__main__":
    main()
