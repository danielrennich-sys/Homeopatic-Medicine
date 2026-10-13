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
import os
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# Both directories can be overridden (e.g. to merge from another checkout's
# processed data, or to write a trial merge somewhere else).
PROCESSED_DIR = Path(os.environ.get("HOMEO_PROCESSED_DIR", ROOT / "data" / "processed"))
MERGED_DIR = Path(os.environ.get("HOMEO_MERGED_DIR", ROOT / "data" / "merged"))
MERGED_DIR.mkdir(parents=True, exist_ok=True)

# Processed files whose "records" are repertory chapters, alphabet headings or
# book front-matter rather than remedies (their ingest scripts need fixing).
JUNK_SOURCE_FILES = {
    # Repertories (symptom -> remedies). derive_repertory_sources.py inverts
    # them into derived_repertories.json, which is merged like any source.
    "boger_synoptic.json",        # repertory chapters: TIME, EYES, COUGH...
    "boger_genera_morborum.json", # "General Analysis By ... Boger"
    "gentry.json",                # "The Concordance Repertory" x23
    # Not a materia medica at all: "The Principles and Art of Cure" (philosophy)
    "roberts_sensations.json",
}
# Headline names that are chapter/section titles, not remedies
JUNK_NAME_RE = re.compile(
    r"^[-—–\s]*(a|an|the|list|index|prefaces?|chapter|general analysis|summary|synoptic|tabulated|"
    r"introduction|characteristic symptoms|note|pages)\b|^([a-z])\2\2$",
    re.I,
)

# Pipeline order after this script:
#   tag_cell_salts.py -> clean_merged.py -> build_search_index.py -> build_passage_index.py

# Genus abbreviations -> full Latin (extend over time)
ABBREVIATION_MAP = {
    "acon": "aconitum",
    "aesc": "aesculus hippocastanum",
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
    "ferr": "ferrum metallicum",
    # Allen's Encyclopedia abbreviations (hyphenated ones used to be cut at
    # the hyphen and merged into a bogus genus record)
    "aesc-g": "aesculus glabra",
    "carb-s": "carboneum sulphuratum",
    "chin-s": "chininum sulphuricum",
    "coc-c": "coccus cacti",
    "euph-ip": "euphorbia ipecacuanhae",
    "kali-i": "kali iodatum",
    "kali-n": "kali nitricum",
    "lac-ac": "lacticum acidum",
    "merc-c": "mercurius corrosivus",
    "merc-sol": "mercurius solubilis",
    "nat-a": "natrum arsenicosum",
    "ol-j": "oleum jecoris aselli",
    "tril-c": "trillium cernuum",
    "digit": "digitalis",
    "dios": "dioscorea villosa",
    "fago": "fagopyrum esculentum",
    "phys": "physostigma venenosum",
    "ptel": "ptelea trifoliata",
    "stry": "strychninum",
    "sumb": "sumbul",
    "tab": "tabacum",
    "ther": "theridion",
    "til": "tilia europaea",
    "vac": "vaccininum",
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
    "echinacea": "echinacea",  # genus-level: angustifolia and purpurea stay distinct
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
    "cascara sagrada": "rhamnus purshiana",
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


ACID_WORDS = {"acid", "acidum", "acidi", "ac"}

# Spelling / nomenclature variants of the first token (genus or substance)
GENUS_SPELLING = {
    "kalium": "kali", "natrium": "natrum", "magnesium": "magnesia", "barium": "baryta",
    "strontia": "strontium", "strontiana": "strontium",
    "actea": "actaea", "aconite": "aconitum", "tarantula": "tarentula", "thuya": "thuja",
    "cinchona": "china", "haematoxylum": "haematoxylon", "erechthites": "erechtites",
    "cenothera": "oenothera", "evonymus": "euonymus", "epigea": "epigaea", "spirea": "spiraea",
    "quillaya": "quillaja", "rocella": "roccella", "cherranthus": "cheiranthus",
    "guajacum": "guaiacum", "thrombidium": "trombidium", "loleum": "lolium", "sarenoa": "serenoa",
    "phaceolus": "phaseolus", "copaiva": "copaiba", "squilla": "scilla", "antimony": "antimonium",
    "cinnamon": "cinnamomum", "coffee": "coffea", "nanthe": "oenanthe", "agrostema": "agrostemma",
    "agrostemna": "agrostemma", "sulfur": "sulphur", "camphor": "camphora", "cantharides": "cantharis",
    "peonia": "paeonia", "hepatica": "hepatica", "sulfuricum": "sulphuricum",
}
EPITHET_SPELLING = {
    "sulfuricum": "sulphuricum", "sulfurica": "sulphurica", "sulfuratum": "sulphuratum",
    "sulfuratus": "sulphuratus", "soccotrina": "socotrina", "candensis": "canadensis",
    "magalis": "majalis", "marjorana": "majorana", "chrysanthemum": "chrysanthum",
    "laterifolia": "lateriflora", "virginica": "virginiana", "zeylanicum": "verum",
    "ceylanicum": "verum", "cann": "cannabinum", "europ": "europaeum", "semper": "sempervirens",
    "vill": "villosa", "rob": "robusta", "acet": "aceticum", "jacob": "jacobaea",
}
# Salts: homeopathic Latin uses feminine epithets for these bases...
FEMININE_BASES = {"magnesia", "calcarea", "baryta", "alumina", "silicea", "strontia"}
# ...and neuter/masculine for these
NEUTER_BASES = {"kali", "natrum", "ammonium", "zincum", "cuprum", "ferrum", "argentum", "aurum",
                "plumbum", "stannum", "manganum", "lithium", "strontium", "cadmium", "niccolum",
                "platinum", "antimonium", "arsenicum", "bismuthum", "cobaltum", "chininum", "uranium"}


def _fold(s: str) -> str:
    s = s.replace("\u00e6", "ae").replace("\u00c6", "AE").replace("\u0153", "oe").replace("\u0152", "OE")
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def _tokens(s: str) -> list[str]:
    toks = re.findall(r"[a-z]+", s.lower())
    toks = [t for t in toks if t not in NOISE_TOKENS]
    out: list[str] = []
    for t in toks:                      # "Abies. Abies canadensis" -> abies canadensis
        if out and out[-1] == t:
            continue
        out.append(t)
    return out


def _epithet_gender(base: str, epithet: str) -> str:
    if base in FEMININE_BASES:
        for a, b in (("icum", "ica"), ("atum", "ata"), ("osum", "osa"), ("idum", "ida")):
            if epithet.endswith(a):
                return epithet[: -len(a)] + b
    elif base in NEUTER_BASES:
        for a, b in (("ica", "icum"), ("ata", "atum"), ("osa", "osum"), ("ida", "idum")):
            if epithet.endswith(a):
                return epithet[: -len(a)] + b
    return epithet


def canonical_key(latin: str) -> str:
    """Reduce a headline name to its canonical Genus-species slug.

    Handles the shapes the sources actually use:
      "Kali-Bi" / "Nat-M"                      Allen's hyphenated abbreviations
      "ABRUS PRECATORIUS -- JEQUIRITY"         Boericke: Latin -- common name
      "Abies. Abies canadensis"                King's/Ellingwood: Genus. Genus species
      "Abrotanum. Southernwood"                Latin. common name
      "BELLADONNA. (ATROPA BELLADONNA.)"       Clarke: parenthetical synonym
      "Acetic acid" / "Acidum aceticum"        acids in any order -> "aceticum-acidum"
      "Kalium bichromicum" / "Natrium mur."    nomenclature variants -> kali / natrum
    """
    if not latin:
        return ""
    raw = _fold(latin).strip()

    # 1. Short hyphenated abbreviation
    m = re.match(r"^([a-z]{1,6})-([a-z]{1,3})\.?$", raw.lower())
    if m:
        key = f"{m.group(1)}-{m.group(2)}"
        if key in ABBREVIATION_MAP:
            return canonical_key(ABBREVIATION_MAP[key])

    s = raw
    # 2. Drop parentheticals ("(U. S. P.)", "(Atropa belladonna)")
    s = re.sub(r"\([^)]*\)", " ", s)
    # 3. Cut common-name / qualifier tails. A hyphen only counts as a separator
    #    when it has spaces around it or is doubled, so "Kali-Bi" and "uva-ursi" survive.
    s = re.split(r"\s+-+\s+|--|[\u2014\u2013,;:]", s, maxsplit=1)[0]
    # 4. "Genus. Genus species" / "Latin. Common name" / "Carbolic. Acid"
    parts = [p for p in re.split(r"\.\s+", s) if p.strip()]
    if len(parts) >= 2:
        t0, t1 = _tokens(parts[0]), _tokens(parts[1])
        if t0 and t1 and t1[0] == t0[0] and len(t1) >= 2:
            s = parts[1]
        elif t0 and t1 and (t1[0] in ACID_WORDS or t0[-1] in ACID_WORDS):
            s = parts[0] + " " + parts[1]
        else:
            s = parts[0]

    tokens = _tokens(s)
    if not tokens:
        return ""

    # 5. Common (English) names -> Latin. Multi-word names may be followed by
    #    extra words ("black cohosh root"); a single-word name must be the whole
    #    thing, so "Echinacea angustifolia" is not swallowed by "echinacea".
    for n in range(min(4, len(tokens)), 0, -1):
        phrase = " ".join(tokens[:n])
        if phrase in COMMON_TO_LATIN and (n >= 2 or n == len(tokens)):
            target = COMMON_TO_LATIN[phrase]
            return slugify(target) if slugify(target) == phrase.replace(" ", "-") else canonical_key(target)

    # 6. Abbreviations ("Bell", "Kali bi")
    if len(tokens) >= 2 and f"{tokens[0]}-{tokens[1]}" in ABBREVIATION_MAP:
        return canonical_key(ABBREVIATION_MAP[f"{tokens[0]}-{tokens[1]}"])
    if len(tokens) == 1 and tokens[0] in ABBREVIATION_MAP:
        return canonical_key(ABBREVIATION_MAP[tokens[0]])

    # 7. Acids: "<substance>um acidum" whatever the source wrote
    if any(t in ACID_WORDS for t in tokens):
        subs = [t for t in tokens if t not in ACID_WORDS]
        if subs:
            sub = subs[0]
            if sub.endswith("ic"):
                sub += "um"
            sub = GENUS_SPELLING.get(sub, EPITHET_SPELLING.get(sub, sub))
            return slugify(f"{sub} acidum")

    # 8. Spelling and nomenclature
    tokens = [GENUS_SPELLING.get(tokens[0], tokens[0])] + [EPITHET_SPELLING.get(t, t) for t in tokens[1:]]
    if tokens[:2] == ["asa", "foetida"]:
        tokens = ["asafoetida"] + tokens[2:]
    if len(tokens) >= 2:
        tokens[1] = _epithet_gender(tokens[0], tokens[1])

    # 9. Genus + species (three tokens for hyphenated species like uva-ursi)
    n = 3 if len(tokens) >= 3 and tokens[1] in {"uva", "agnus", "bursa"} else 2
    return slugify(" ".join(tokens[:n]))


def title_name(name: str) -> str:
    """'ACETICUM ACIDUM' / 'Aceticum acidum' -> 'Aceticum Acidum' (keeps hyphens)."""
    return re.sub(r"[A-Za-z\u00c0-\u024f]+", lambda m: m.group(0).capitalize(), name.strip())


def display_name(key: str, headline_names: list[str]) -> str:
    """Pick the remedy's display name: a source headline that IS the canonical
    name if one exists (keeps e.g. 'X-Ray'), otherwise the key in Title Case."""
    for n in headline_names:
        if n and slugify(_fold(n)) == key:
            return title_name(n)
    return " ".join(w.capitalize() for w in key.split("-"))


CATEGORY_PRIORITY = ["homeopathic", "herbal", "supplement", "nutrient", "mineral", "animal", "nosode"]


def _item_key(item):
    if isinstance(item, dict):
        return (item.get("text", "")[:200], item.get("source_id") or item.get("source") or "")
    return (str(item)[:200], "")


def _extend_dedup(target: list, items: list) -> None:
    seen = {_item_key(x) for x in target}
    for it in items:
        key = _item_key(it)
        if key in seen:
            continue
        seen.add(key)
        target.append(it)


def merge_traditional(target: dict, addition: dict) -> None:
    """Merge a traditional/evidence block into another without losing text.

    Sources disagree about shapes: Hering's "modalities" is a list of texts,
    Boericke's is a dict of better_from/worse_from lists, Grieve's "dosing" is
    a list where Boericke's is a dict with a "notes" string, NCCIH's "summary"
    is a string. Every combination is kept: lists concatenate (deduplicated),
    dicts merge recursively, strings append, and a list or string meeting a
    dict goes under the dict's "entries" / "notes".
    """
    for k, v in addition.items():
        if v in (None, "", [], {}):
            continue
        if k not in target or target[k] in (None, "", [], {}):
            target[k] = v
            continue
        t = target[k]
        if isinstance(v, list) and isinstance(t, list):
            _extend_dedup(t, v)
        elif isinstance(v, dict) and isinstance(t, dict):
            merge_traditional(t, v)
        elif isinstance(v, str) and isinstance(t, str):
            if v.strip() and v.strip()[:100] not in t:
                target[k] = t.rstrip() + "\n\n" + v.strip()
        elif isinstance(v, str) and isinstance(t, list):
            _extend_dedup(t, [v])
        elif isinstance(v, list) and isinstance(t, str):
            target[k] = [t] + v
        elif isinstance(v, list) and isinstance(t, dict):
            _extend_dedup(t.setdefault("entries", []), v)
        elif isinstance(v, dict) and isinstance(t, list):
            merged = {"entries": t}
            merge_traditional(merged, v)
            target[k] = merged
        elif isinstance(v, str) and isinstance(t, dict):
            merge_traditional(t, {"notes": v})
        elif isinstance(v, dict) and isinstance(t, str):
            merged = {"notes": t}
            merge_traditional(merged, v)
            target[k] = merged
        # else: non-text scalars (numbers, bools) — keep first


def merge_records(records: list[dict], key: str = "") -> dict:
    """Combine a group of records sharing one canonical key."""
    base = {
        "id": key,
        "names": {"primary": "", "latin": "", "common": [], "synonyms": []},
        "category": "homeopathic",
        "categories": [],
        "traditional": {},
        "evidence": {},
        "provenance": {"sources": []},
    }
    seen_sources = []
    headline_names = []
    for r in records:
        names = r.get("names", {}) or {}
        for n in (names.get("primary"), names.get("latin")):
            n = (n or "").strip()
            if n and n not in headline_names:
                headline_names.append(n)
        for c in names.get("common", []) or []:
            if c and c.lower() not in {x.lower() for x in base["names"]["common"]}:
                base["names"]["common"].append(c)
        cat = r.get("category")
        if cat and cat not in base["categories"]:
            base["categories"].append(cat)
        merge_traditional(base["traditional"], r.get("traditional", {}) or {})
        merge_traditional(base["evidence"], r.get("evidence", {}) or {})
        for src in (r.get("provenance", {}) or {}).get("sources", []) or []:
            if src not in seen_sources:
                seen_sources.append(src)
    # One clean display name; every headline the sources used stays searchable
    primary = display_name(key, headline_names) if key else (headline_names[0] if headline_names else "")
    base["names"]["primary"] = primary
    base["names"]["latin"] = primary
    base["names"]["synonyms"] = [n for n in headline_names if n.lower() != primary.lower()]
    # A remedy that is in a homeopathic materia medica is "homeopathic" even if
    # the NIH also lists the herb; the full list is kept in `categories`.
    base["categories"].sort(key=lambda c: CATEGORY_PRIORITY.index(c) if c in CATEGORY_PRIORITY else 99)
    base["category"] = base["categories"][0] if base["categories"] else "homeopathic"
    base["provenance"]["sources"] = seen_sources
    return base


def main() -> None:
    files = sorted(PROCESSED_DIR.glob("*.json"))
    if not files:
        print("no source files in", PROCESSED_DIR)
        sys.exit(1)

    groups: dict[str, list[dict]] = defaultdict(list)
    skipped_no_key = 0
    skipped_junk = 0
    total_in = 0
    per_source_counts: dict[str, int] = {}

    for f in files:
        if f.name in JUNK_SOURCE_FILES:
            print(f"  skipping {f.name}: not remedy records")
            continue
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
            if JUNK_NAME_RE.match(latin.strip()):
                skipped_junk += 1
                continue
            key = canonical_key(latin)
            if not key:
                skipped_no_key += 1
                continue
            groups[key].append(rec)

    # Start clean so remedies that no longer exist under an old id disappear
    stale = [p for p in MERGED_DIR.glob("*.json")]
    for p in stale:
        p.unlink()
    print(f"Cleared {len(stale)} old files from {MERGED_DIR}")

    # Merge each group
    merged_index = []
    for key, recs in sorted(groups.items()):
        merged = merge_records(recs, key)
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
            "skipped_junk_headings": skipped_junk,
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
    print(f"Skipped (no canonical key): {skipped_no_key}; skipped (chapter/heading names): {skipped_junk}")
    print(f"Canonical remedies: {len(merged_index)}")
    print(f"  in 2+ sources: {multi}")
    print(f"Wrote -> {MERGED_DIR}")


if __name__ == "__main__":
    main()
