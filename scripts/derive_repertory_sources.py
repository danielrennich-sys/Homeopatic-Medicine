"""
Turn symptom-organised books into per-remedy records.

Some sources are not organised by remedy, so the per-remedy ingest produced
records named after chapters ("HEAD", "COUGH", "The Ophidia"). This script
reads those processed files and writes per-remedy records that
merge_remedies.py picks up like any other source:

  boger_synoptic.json          Boger, Synoptic Key - repertory (rubric : remedies)
  boger_genera_morborum.json   Boger, General Analysis - repertory
  gentry.json                  Gentry, Concordance Repertory (symptom :- remedies)
  farrington.json              the group lectures ("The Ophidia", "The Acids"...)
  grieve_modern_herbal.json    family overview pages ("N.O. Compositae ...")

Repertories are inverted: every rubric that lists a remedy becomes part of
that remedy's `repertory` section ("HEAD: Head, forehead, eyes, over, right").
Lectures and overview pages are attributed paragraph by paragraph to the
remedies each paragraph names (`comparative` section, tagged with the lecture).

Abbreviations are resolved with the books' own abbreviation tables (Boger's
"List of Remedies", Allen's Handbook index) plus merge_remedies.ABBREVIATION_MAP.

Output: <processed dir>/derived_repertories.json and derived_lectures.json
Run before merge_remedies.py (same HOMEO_PROCESSED_DIR).
"""
from __future__ import annotations

import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from merge_remedies import ABBREVIATION_MAP, JUNK_NAME_RE, canonical_key, _fold  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = Path(os.environ.get("HOMEO_PROCESSED_DIR", ROOT / "data" / "processed"))

# Remedy abbreviations the books' own tables do not cover
EXTRA_ABBREVIATIONS = {
    "aco": "aconitum napellus", "pho": "phosphorus", "sul": "sulphur", "pul": "pulsatilla",
    "glo": "glonoinum", "chin": "china officinalis", "calc-c": "calcarea carbonica",
    "kali-io": "kali iodatum", "kali-bi": "kali bichromicum", "nat-m": "natrum muriaticum",
    "nat-c": "natrum carbonicum", "nat-s": "natrum sulphuricum", "nux-v": "nux vomica",
    "nux-m": "nux moschata", "rhus-t": "rhus toxicodendron", "pho-ac": "phosphoricum acidum",
    "mur-ac": "muriaticum acidum", "nit-ac": "nitricum acidum", "sul-ac": "sulphuricum acidum",
    "fl-ac": "fluoricum acidum", "pic-ac": "picricum acidum", "ox-ac": "oxalicum acidum",
    "carb-v": "carbo vegetabilis", "carb-an": "carbo animalis", "carb-s": "carboneum sulphuratum",
    "merc-c": "mercurius corrosivus", "merc-i-f": "mercurius iodatus flavus",
    "merc-i-r": "mercurius iodatus ruber", "merc-cy": "mercurius cyanatus", "merc": "mercurius",
    "hep": "hepar sulphuris", "lil-t": "lilium tigrinum", "aru-t": "arum triphyllum",
    "pru-s": "prunus spinosa", "cinnb": "cinnabaris", "stap": "staphysagria", "stict": "sticta pulmonaria",
    "cof": "coffea cruda", "con": "conium maculatum", "cup": "cuprum metallicum", "hyo": "hyoscyamus niger",
    "zin": "zincum metallicum", "fer": "ferrum metallicum", "kre": "kreosotum", "sabi": "sabina",
    "saba": "sabadilla", "thu": "thuja occidentalis", "verb": "verbascum thapsus", "ced": "cedron",
    "par": "paris quadrifolia", "cam": "camphora", "cist": "cistus canadensis", "hyds": "hydrastis canadensis",
    "rum": "rumex crispus", "seneg": "senega", "cimi": "cimicifuga racemosa", "bap": "baptisia tinctoria",
    "ver-v": "veratrum viride", "eup-per": "eupatorium perfoliatum", "gel": "gelsemium sempervirens",
    "dul": "dulcamara", "caus": "causticum", "grap": "graphites", "colo": "colocynthis", "stan": "stannum metallicum",
    "tub": "tuberculinum", "psor": "psorinum", "syph": "syphilinum", "med": "medorrhinum", "tell": "tellurium",
    "alu": "alumina", "amb": "ambra grisea", "ap": "apis mellifica", "aran": "aranea diadema",
    "podo": "podophyllum peltatum", "sel": "selenium", "sang": "sanguinaria canadensis", "camph": "camphora",
    "cupr": "cuprum metallicum", "calc-p": "calcarea phosphorica", "mag-p": "magnesia phosphorica",
    "mag-m": "magnesia muriatica", "mag-s": "magnesia sulphurica", "kali-s": "kali sulphuricum",
    "kali-m": "kali muriaticum", "kali-br": "kali bromatum", "kali-ar": "kali arsenicosum",
    "nat-a": "natrum arsenicosum", "nat-p": "natrum phosphoricum", "am-m": "ammonium muriaticum",
    "am-c": "ammonium carbonicum", "ars-io": "arsenicum iodatum", "ars-h": "arsenicum hydrogenisatum",
    "bar-m": "baryta muriatica", "bar-c": "baryta carbonica", "aur-m": "aurum muriaticum",
    "arg-m": "argentum metallicum", "arg-n": "argentum nitricum", "lac-c": "lac caninum",
    "lac-d": "lac defloratum", "chin-s": "chininum sulphuricum", "chin-a": "chininum arsenicosum",
    "crot-h": "crotalus horridus", "crot-t": "croton tiglium", "plb": "plumbum metallicum",
    "plat": "platinum metallicum", "ant-t": "antimonium tartaricum", "ant-c": "antimonium crudum",
    "calc-f": "calcarea fluorica", "calc-s": "calcarea sulphurica", "calc-ar": "calcarea arsenicosa",
    "coloc": "colocynthis", "ign": "ignatia amara", "lach": "lachesis", "lyc": "lycopodium clavatum",
    "sep": "sepia", "sil": "silicea", "spig": "spigelia anthelmia", "stram": "stramonium",
    "iris": "iris versicolor", "naj": "naja tripudians", "bov": "bovista", "mez": "mezereum",
    "asaf": "asafoetida", "aur": "aurum metallicum", "bism": "bismuthum", "chel": "chelidonium majus",
    "cycl": "cyclamen europaeum", "phyt": "phytolacca decandra", "anac": "anacardium orientale",
    "ail": "ailanthus glandulosa", "agar": "agaricus muscarius", "bell": "belladonna", "bry": "bryonia alba",
    "cham": "chamomilla", "cocc": "cocculus indicus", "colch": "colchicum autumnale", "dios": "dioscorea villosa",
    "ferr": "ferrum metallicum", "gels": "gelsemium sempervirens", "guai": "guaiacum officinale",
    "iod": "iodum", "led": "ledum palustre", "op": "opium", "petr": "petroleum", "ruta": "ruta graveolens",
    "sec": "secale cornutum", "spong": "spongia tosta", "tarent": "tarentula hispanica", "verat": "veratrum album",
    "caps": "capsicum", "canth": "cantharis", "cic": "cicuta virosa", "cina": "cina maritima", "dig": "digitalis purpurea",
    "dros": "drosera rotundifolia", "euph": "euphorbium officinarum", "hell": "helleborus niger", "ham": "hamamelis virginiana",
    "kali-c": "kali carbonicum", "kali-p": "kali phosphoricum", "kali-n": "kali nitricum", "mag-c": "magnesia carbonica",
    "ars": "arsenicum album", "arn": "arnica montana", "calc": "calcarea carbonica", "phos": "phosphorus",
    "puls": "pulsatilla", "sulph": "sulphur", "nat-mur": "natrum muriaticum", "coca": "coca", "tab": "tabacum",
    # seen unresolved in Boger / Gentry
    "coccl": "cocculus indicus", "cyc": "cyclamen europaeum", "latro": "latrodectus mactans",
    "elap": "elaps corallinus", "squ": "scilla maritima", "pareir": "pareira brava", "hypr": "hypericum perforatum",
    "ipec": "ipecacuanha", "illic": "illicium anisatum", "mrx": "murex purpurea", "xanth": "xanthoxylum fraxineum",
    "carb-a": "carbo animalis", "stic": "sticta pulmonaria", "calc-hy": "calcarea hypophosphorosa",
    "eupho": "euphorbium officinarum", "buf": "bufo", "lyss": "lyssinum", "cinb": "cinnabaris",
    "spi": "spigelia anthelmia", "poly-s": "polyporus", "chel": "chelidonium majus",
}

SRC = {
    "boger_synoptic.json": "boger-synoptic-key-1915",
    "boger_genera_morborum.json": "boger-genera-morborum-1907",
    "gentry.json": "gentry-rubrical-mm-1890",
    "farrington.json": "farrington-clinical-1908",
    "grieve_modern_herbal.json": "grieve-modern-herbal-1931",
}

ABBR_TOKEN = re.compile(r"^\(?([A-Za-z][A-Za-z\-]*)\)?\s?(\.?)(,?)\)?$")


def load(name):
    p = PROCESSED_DIR / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else []


def record_text(rec) -> str:
    parts = []
    for v in (rec.get("traditional") or {}).values():
        if isinstance(v, list):
            parts += [it["text"] for it in v if isinstance(it, dict) and it.get("text")]
    return "\n\n".join(parts)


# ---------------------------------------------------------------- abbreviations
def norm_abbr(tok: str) -> str:
    t = _fold(tok).lower().strip("().,;: ")
    t = re.sub(r"\s*\.\s*", "", t)      # "acet. ac." -> "acetac"? no: keep hyphen style
    return t.strip("-")


def build_abbreviation_table() -> dict[str, str]:
    table: dict[str, str] = {}
    # Boger's "List of Remedies in the Repertory": "Ab-c. Abies Canadensis Abies-n. Abies Nigra ..."
    for rec in load("boger_synoptic.json"):
        if "List of Remedies" not in (rec["names"].get("latin") or ""):
            continue
        text = record_text(rec)
        text = text.split("Abbreviation Remedy", 1)[-1]
        tokens = text.split()
        cur_abbr, cur_name = None, []
        for tok in tokens:
            if re.match(r"^[A-Z][A-Za-z\-]*\.$", tok) and len(tok) <= 12:
                if cur_abbr and cur_name:
                    table[norm_abbr(cur_abbr)] = " ".join(cur_name)
                cur_abbr, cur_name = tok, []
            elif re.match(r"^[A-Z]$", tok):      # section letter
                continue
            elif cur_abbr:
                cur_name.append(tok)
        if cur_abbr and cur_name:
            table[norm_abbr(cur_abbr)] = " ".join(cur_name)
    # Allen's Handbook letter pages (cached raw HTML): "Abies Canadensis ---------- Abies-c ."
    raw_dir = Path(os.environ.get("HOMEO_RAW_DIR", ROOT / "data" / "raw")) / "allen_handbook"
    for page in sorted(raw_dir.glob("[a-z].htm")) if raw_dir.exists() else []:
        html = page.read_text(encoding="iso-8859-1", errors="replace")
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html)).replace("&nbsp;", " ")
        for name, abbr in re.findall(r"([A-Z][A-Za-z\u00c6\u00e6 ,.()'-]{2,60}?)\s+-{4,}\s+([A-Za-z][A-Za-z\-]*)\s*\.?(?=\s)", text):
            table.setdefault(norm_abbr(abbr), name.strip(" ."))
    for k, v in ABBREVIATION_MAP.items():
        table.setdefault(norm_abbr(k), v)
    for k, v in EXTRA_ABBREVIATIONS.items():
        table.setdefault(norm_abbr(k), v)
    return table


class Resolver:
    def __init__(self):
        self.table = build_abbreviation_table()
        self.unresolved = Counter()
        self.cache = {}

    def resolve(self, tok: str) -> tuple[str, str] | None:
        """abbreviation token -> (canonical key, full name)"""
        a = norm_abbr(tok)
        if not a or len(a) < 2:
            return None
        if a in self.cache:
            return self.cache[a]
        name = self.table.get(a)
        if not name:
            # "calc-c" ~ "calc-carb", "kali-io" ~ "kali-i": try shortening the suffix
            if "-" in a:
                base, suf = a.rsplit("-", 1)
                for n in range(len(suf), 0, -1):
                    if f"{base}-{suf[:n]}" in self.table:
                        name = self.table[f"{base}-{suf[:n]}"]
                        break
            if not name and a in self.table:
                name = self.table[a]
        if not name:
            # full names written out ("Bufo", "Aloe", "Coca")
            key = canonical_key(tok.strip("().,; "))
            if key and len(key) >= 4 and "-" not in key and key in KNOWN_KEYS:
                name = tok.strip("().,; ")
        if not name and len(a) >= 4:
            # unique prefix of a known remedy ("pareir" -> pareira-brava)
            parts = a.split("-")
            cands = [k for k in KNOWN_KEYS if k.startswith(parts[0]) and
                     (len(parts) == 1 or ("-" in k and k.split("-", 1)[1].startswith(parts[1])))]
            genera = {c.split("-")[0] for c in cands}
            if len(cands) == 1:
                name = cands[0].replace("-", " ")
            elif len(parts) == 1 and len(genera) == 1 and parts[0] in KNOWN_KEYS:
                name = parts[0]
        if not name:
            self.unresolved[a] += 1
            self.cache[a] = None
            return None
        key = canonical_key(name)
        res = (key, name) if key else None
        self.cache[a] = res
        return res


KNOWN_KEYS: set[str] = set()


def learn_known_keys():
    for f in PROCESSED_DIR.glob("*.json"):
        if f.name.startswith("derived_"):
            continue
        try:
            for rec in json.loads(f.read_text(encoding="utf-8")):
                latin = (rec.get("names") or {}).get("latin") or ""
                if JUNK_NAME_RE.match(latin.strip()):
                    continue
                k = canonical_key(latin)
                if k:
                    KNOWN_KEYS.add(k)
        except Exception:
            pass


# ---------------------------------------------------------------- rubric parsing
def parse_remedy_list(tokens: list[str], start: int, resolver: Resolver):
    """Consume remedy abbreviations from tokens[start:]. Returns (remedies, next_index)."""
    rems = []
    i = start
    while i < len(tokens):
        tok = tokens[i]
        if tok in (",", ".,", ";"):
            i += 1
            continue
        if tok == ".":
            i += 1
            break
        m = ABBR_TOKEN.match(tok)
        if not m:
            break
        name, dot, comma = m.group(1), m.group(2), m.group(3)
        # look-ahead for detached punctuation: "Agar ., arn."
        nxt = tokens[i + 1] if i + 1 < len(tokens) else ""
        continues = bool(comma) or nxt in (",", ".,")
        ends = (dot and not comma) or nxt == "."
        if not comma and not dot and nxt not in (",", ".,", "."):
            # bare word: a remedy only if it resolves, otherwise it starts the next rubric
            if resolver.resolve(name) is None or name[0].islower():
                break
            ends = True
        r = resolver.resolve(name)
        if r:
            rems.append(r)
        i += 1
        if nxt in (",", ".,", "."):
            i += 1
        if ends and not continues:
            break
    return rems, i


def parse_repertory_chapter(text: str, chapter: str, resolver: Resolver, sep_regex: str):
    """Yield (rubric, [(key, name)...]) from "rubric : Rem1., Rem2." text."""
    pieces = re.split(sep_regex, text)
    carry = pieces[0]
    for piece in pieces[1:]:
        tokens = piece.split()
        rems, i = parse_remedy_list(tokens, 0, resolver)
        rubric = carry.strip(" ,;:-")
        # keep only the last sentence of the carry as the rubric name
        rubric = re.split(r"(?<=[.!?])\s+(?=[A-Z])", rubric)[-1].strip()
        if rems and 2 <= len(rubric) <= 200 and not rubric.lower().startswith("see "):
            yield rubric, rems
        carry = " ".join(tokens[i:])


def invert_boger(resolver: Resolver, out: dict, fname: str, src: str):
    n_rubrics = 0
    for rec in load(fname):
        chapter = (rec["names"].get("latin") or "").strip()
        if chapter.startswith("A SYNOPTIC") or "List of Remedies" in chapter or chapter.startswith("General Analysis By"):
            chapter = "GENERAL ANALYSIS" if "General Analysis" in chapter else None
            if chapter is None:
                continue
        text = record_text(rec)
        # Drop the page header ("A SYNOPTIC KEY ... REPERTORY HEAD.") and letter navigation
        head = text[:600]
        m = re.search(r"REPERTORY\s+" + re.escape(chapter) + r"\s*\.?\s*", head) or re.search(re.escape(chapter) + r"\s*\.?\s+(?=[A-Z])", head)
        if m:
            text = text[m.end():]
        text = re.sub(r"^\s*(?:\*\s*[A-Z]\s*)+\*?\s*", "", text, count=1)
        per_remedy = defaultdict(list)
        for rubric, rems in parse_repertory_chapter(text, chapter, resolver, r"\s:\s|(?<=etc\.):\s|(?<=[a-z\.]):(?=\s[A-Z])"):
            n_rubrics += 1
            rubric = re.sub(r"\s+([,;.])", r"\1", rubric)
            for key, name in rems:
                per_remedy[(key, name)].append(rubric)
        for (key, name), rubrics in per_remedy.items():
            out[key]["name"] = out[key].get("name") or name
            out[key]["items"].append({"text": f"{chapter.title()}: " + "; ".join(dict.fromkeys(rubrics)) + ".",
                                      "source_id": src, "chapter": chapter.title()})
            out[key]["sources"].add(src)
    return n_rubrics


def invert_gentry(resolver: Resolver, out: dict):
    src = SRC["gentry.json"]
    n = 0
    for rec in load("gentry.json"):
        text = record_text(rec)
        chapter = "Generalities"
        headword = ""
        pending = ""
        segments = re.split(r"\s:-\s", text)
        pending = segments[0]
        per_remedy = defaultdict(list)
        for seg in segments[1:]:
            tokens = seg.split()
            rems, i = parse_remedy_list(tokens, 0, resolver)
            rest = " ".join(tokens[i:])
            if rems:
                symptom = pending.strip(" ;:,.")
                m = re.search(r"\*+\s*CHAPTER\s*:\s*([A-Z ,&]+?)\s*\*+", symptom)
                if m:
                    chapter = m.group(1).strip().title()
                    symptom = symptom[m.end():].strip()
                symptom = re.split(r"(?<=[.!?])\s+(?=[A-Z])", symptom)[-1]
                if 3 <= len(symptom) <= 400:
                    n += 1
                    label = f"{headword}: {symptom}" if headword and not symptom.lower().startswith(headword.lower()) else symptom
                    for key, name in rems:
                        per_remedy[(key, name)].append((chapter, label))
                pending = rest
            else:
                m = re.search(r"\*+\s*CHAPTER\s*:\s*([A-Z ,&]+?)\s*\*+", rest)
                if m:
                    chapter = m.group(1).strip().title()
                    rest = rest[m.end():].strip()
                words = re.sub(r"\([^)]*\)", "", pending).split()
                if 0 < len(words) <= 4:
                    headword = " ".join(words).strip(" :;,.")
                pending = rest
        for (key, name), entries in per_remedy.items():
            by_ch = defaultdict(list)
            for ch, label in entries:
                by_ch[ch].append(label)
            out[key]["name"] = out[key].get("name") or name
            for ch, labels in by_ch.items():
                out[key]["items"].append({"text": f"{ch}: " + " | ".join(dict.fromkeys(labels)),
                                          "source_id": src, "chapter": ch})
            out[key]["sources"].add(src)
    return n


# ---------------------------------------------------------------- lectures / overviews
def build_name_index() -> dict[str, str]:
    """lowercase remedy name -> canonical key, from every processed record's names."""
    idx = {}
    for f in PROCESSED_DIR.glob("*.json"):
        if f.name.startswith("derived_") or f.name in ("boger_synoptic.json", "gentry.json", "roberts_sensations.json"):
            continue
        try:
            recs = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        for rec in recs:
            names = rec.get("names") or {}
            for n in [names.get("latin"), names.get("primary")] + list(names.get("synonyms") or []):
                if not n or JUNK_NAME_RE.match(n.strip()):
                    continue
                clean = re.sub(r"\([^)]*\)", " ", _fold(n)).split(",")[0].split(" -")[0].strip(" .")
                key = canonical_key(n)
                if key and 4 <= len(clean) <= 40 and re.match(r"^[A-Za-z][A-Za-z\- ]+$", clean):
                    idx.setdefault(clean.lower(), key)
    # A genus on its own ("Lachesis", "Naja") only when a genus-level record exists;
    # never guess a species for "Ammonium" or "Apocynum".
    for k in list(KNOWN_KEYS):
        if "-" not in k and len(k) >= 4:
            idx.setdefault(k, k)
    return idx


def attribute_paragraphs(text: str, name_index: dict[str, str]) -> dict[str, list[str]]:
    """Attribute each paragraph to the remedies it names (carrying the last ones forward)."""
    multi = sorted((n for n in name_index if " " in n), key=len, reverse=True)
    singles = {n for n in name_index if " " not in n}
    out = defaultdict(list)
    current: list[str] = []
    mentions = Counter()   # how often each remedy is named in this text
    for para in re.split(r"\n\s*\n", text):
        para = para.strip()
        if len(para) < 80:
            continue
        low = _fold(para).lower()
        found = []
        for n in multi:
            if n in low:
                found.append(name_index[n])
        for w in re.findall(r"[A-Z][a-z]{3,}", para):
            k = name_index.get(_fold(w).lower())
            if k and k not in found:
                found.append(k)
        if found:
            # a paragraph that names remedies belongs to them
            current = found[:4]
            mentions.update(current)
            targets = current
        else:
            # an unnamed paragraph continues the lecture's subject remedies, not a
            # remedy that was mentioned once in passing as a comparison
            targets = [k for k in current if mentions[k] >= 2]
        for k in targets:
            out[k].append(para)
    return out


def main() -> None:
    learn_known_keys()
    resolver = Resolver()
    print(f"abbreviation table: {len(resolver.table)} entries")

    rep: dict[str, dict] = defaultdict(lambda: {"name": "", "items": [], "sources": set()})
    n1 = invert_boger(resolver, rep, "boger_synoptic.json", SRC["boger_synoptic.json"])
    n2 = invert_boger(resolver, rep, "boger_genera_morborum.json", SRC["boger_genera_morborum.json"])
    n3 = invert_gentry(resolver, rep)
    print(f"rubrics: Boger Synoptic {n1}, Boger Genera {n2}, Gentry {n3}; remedies with rubrics: {len(rep)}")
    print(f"unresolved abbreviations ({len(resolver.unresolved)}): "
          f"{resolver.unresolved.most_common(40)}")

    records = []
    for key, d in sorted(rep.items()):
        if not d["items"]:
            continue
        records.append({
            "names": {"primary": d["name"], "latin": d["name"], "common": [], "synonyms": []},
            "category": "homeopathic",
            "traditional": {"repertory": d["items"]},
            "evidence": {},
            "provenance": {"sources": sorted(d["sources"])},
        })
    (PROCESSED_DIR / "derived_repertories.json").write_text(json.dumps(records, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"wrote derived_repertories.json: {len(records)} remedies")

    # Lectures and family overview pages
    name_index = build_name_index()
    lec: dict[str, dict] = defaultdict(lambda: {"items": [], "sources": set()})
    n_par = 0
    for rec in load("farrington.json"):
        title = re.sub(r"\s+", " ", rec["names"].get("latin") or "").strip()
        if not title.lower().startswith("the "):
            continue
        for key, paras in attribute_paragraphs(record_text(rec), name_index).items():
            for p in paras:
                lec[key]["items"].append({"text": f"[{title}] {p}", "source_id": SRC["farrington.json"], "lecture": title})
                n_par += 1
            lec[key]["sources"].add(SRC["farrington.json"])
    for rec in load("grieve_modern_herbal.json"):
        title = (rec["names"].get("latin") or "").strip()
        if not title.startswith("N.O."):
            continue
        short = (rec["names"].get("common") or [title])[0]
        for key, paras in attribute_paragraphs(record_text(rec), name_index).items():
            for p in paras:
                lec[key]["items"].append({"text": f"[{short} overview] {p}", "source_id": SRC["grieve_modern_herbal.json"], "lecture": short})
                n_par += 1
            lec[key]["sources"].add(SRC["grieve_modern_herbal.json"])
    lrecords = []
    for key, d in sorted(lec.items()):
        name = " ".join(w.capitalize() for w in key.split("-"))
        lrecords.append({
            "names": {"primary": name, "latin": name, "common": [], "synonyms": []},
            "category": "homeopathic",
            "traditional": {"comparative": d["items"]},
            "evidence": {},
            "provenance": {"sources": sorted(d["sources"])},
        })
    (PROCESSED_DIR / "derived_lectures.json").write_text(json.dumps(lrecords, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"wrote derived_lectures.json: {len(lrecords)} remedies, {n_par} paragraphs attributed")


if __name__ == "__main__":
    main()
