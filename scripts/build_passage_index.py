"""
Build a passage-level full-text index over the merged remedy files.

Why: the old search_index.json only kept the first few hundred characters of
each section, and matched terms anywhere in a remedy. This index covers the
whole text and lets the web UI find passages where the query terms occur
TOGETHER, then quote those passages back to the user (and to the AI reader).

Output (data/pidx/):
  _meta.json   remedies, section names, passage boundaries, auto-stopwords
  <xx>.json    postings for every stem starting with "xx" (2-letter shard),
               or <xxx>.json for prefixes that would otherwise be too large.
               Format: {stem: [pid_delta, tf, pid_delta, tf, ...]}

Passage ids are assigned sequentially: remedies in id order, sections in file
order, items in list order, passages in text order. Each remedy therefore owns
a contiguous pid range, which makes "score this one remedy" cheap.

Offsets are UTF-16 code units so JavaScript's string.slice() can use them.

The tokenizer/stemmer here MUST stay in sync with js/engine.js (tokenize()).
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MERGED_DIR = ROOT / "data" / "merged"
OUT_DIR = ROOT / "data" / "pidx"

TARGET_LEN = 320      # aim for passages around this many chars
MAX_LEN = 650         # hard-split anything longer than this
SHARD_SPLIT_BYTES = 1_200_000  # 2-letter shards bigger than this become 3-letter shards
AUTO_STOP_FRACTION = 0.45      # stems in more than this fraction of passages are dropped

# Sections that describe the substance rather than symptoms. They are indexed
# (so the detail view can quote them) but flagged so the symptom search skips them.
NON_SYMPTOM_SECTIONS = {"habitat", "constituents", "preparation", "dosing", "dosing.notes",
                        "relationships", "relationships._raw"}

STOP_WORDS = set("""
the and for with from that this are not but was has had have been were also more
very than all can its will may after before during about into over such each which
their other being gets like side does one two who when some there then only just
much most even well use used often upon many are was were his her him she they them
these those what where why how any own out off per via yet
""".split())

SENT_SPLIT = re.compile(r"(?<=[.;!?])\s+|\s*►\s*|\n{2,}")
WORD_RE = re.compile(r"[a-z]+")


def fold(s: str) -> str:
    """Lowercase, fold accents/ligatures to ASCII letters."""
    s = s.lower().replace("æ", "ae").replace("œ", "oe")
    s = unicodedata.normalize("NFD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))


def _strip_suffix(w: str) -> str:
    n = len(w)
    if w.endswith("ies") and n > 4:
        return w[:-3] + "y"
    if w.endswith("ied") and n > 4:
        return w[:-3] + "y"
    if w.endswith("sses"):
        return w[:-2]
    for suf in ("ations", "ation", "ating", "ated", "ates"):
        if w.endswith(suf) and n - len(suf) >= 3:
            return w[:-len(suf)] + "at"
    for suf in ("ing", "ed"):
        if w.endswith(suf) and n - len(suf) >= 3:
            r = w[:-len(suf)]
            if len(r) >= 4 and r[-1] == r[-2] and r[-1] not in "lsz":
                r = r[:-1]
            return r
    if w.endswith("ness") and n - 4 >= 3:
        return w[:-4]
    if w.endswith(("ss", "us", "is")):
        return w
    if w.endswith("es") and n > 4 and (w[-3] in "xz" or w[-4:-2] in ("ch", "sh")):
        return w[:-2]
    if w.endswith("s") and n > 3:
        return w[:-1]
    return w


def stem(w: str) -> str:
    """Small deterministic stemmer, mirrored in js/engine.js (stemWord)."""
    if len(w) <= 3:
        return w
    r = _strip_suffix(w)
    # Normalise endings so inflections land on the same stem:
    # dizzi/dizzy -> dizzy; headache/headaches -> headach; move/moving -> mov
    if r.endswith("i") and len(r) > 3:
        r = r[:-1] + "y"
    if r.endswith("e") and len(r) >= 4:
        r = r[:-1]
    return r


def tokenize(text: str) -> list[str]:
    out = []
    for w in WORD_RE.findall(fold(text)):
        if len(w) < 3 or len(w) > 25 or w in STOP_WORDS:
            continue
        s = stem(w)
        if len(s) >= 3:
            out.append(s)
    return out


def u16len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def split_passages(text: str) -> list[tuple[int, int]]:
    """Return (start, end) UTF-16 offsets of passages within text."""
    # Sentence boundaries as code-point offsets first
    pieces = []
    pos = 0
    for m in SENT_SPLIT.finditer(text):
        if m.start() > pos:
            pieces.append((pos, m.start()))
        pos = m.end()
    if pos < len(text):
        pieces.append((pos, len(text)))
    # Hard-split overlong sentences at word boundaries
    sents: list[tuple[int, int]] = []
    for a, b in pieces:
        while b - a > MAX_LEN:
            cut = text.rfind(" ", a + TARGET_LEN, a + MAX_LEN)
            if cut == -1:
                cut = a + MAX_LEN
            sents.append((a, cut))
            a = cut + 1
        if b > a:
            sents.append((a, b))
    # Greedy pack into passages near TARGET_LEN
    passages: list[tuple[int, int]] = []
    cur_a = None
    cur_b = None
    for a, b in sents:
        if cur_a is None:
            cur_a, cur_b = a, b
        elif (b - cur_a) <= TARGET_LEN:
            cur_b = b
        else:
            passages.append((cur_a, cur_b))
            cur_a, cur_b = a, b
    if cur_a is not None:
        passages.append((cur_a, cur_b))
    # Convert code-point offsets to UTF-16 offsets (cheap: prefix lengths)
    out = []
    for a, b in passages:
        if text[a:b].strip():
            out.append((u16len(text[:a]), u16len(text[:b])))
    return out


def iter_texts(trad: dict):
    """Yield (section_path, item_index, text) for every text in a traditional block."""
    for key, val in trad.items():
        if isinstance(val, list):
            for i, item in enumerate(val):
                if isinstance(item, dict):
                    t = item.get("text")
                    if isinstance(t, str) and t.strip():
                        yield key, i, t
                elif isinstance(item, str) and item.strip():
                    yield key, i, item
        elif isinstance(val, dict):
            for sk, sv in val.items():
                path = f"{key}.{sk}"
                if isinstance(sv, list):
                    for i, item in enumerate(sv):
                        if isinstance(item, dict):
                            t = item.get("text")
                            if isinstance(t, str) and t.strip():
                                yield path, i, t
                        elif isinstance(item, str) and item.strip():
                            yield path, i, item
                elif isinstance(sv, str) and sv.strip():
                    yield path, -1, sv
        elif isinstance(val, str) and val.strip():
            yield key, -1, val


def main() -> None:
    files = sorted(f for f in MERGED_DIR.glob("*.json") if f.name != "_index.json")
    print(f"Indexing {len(files)} merged files")

    remedies = []
    sections: dict[str, int] = {}
    items = []           # [remIdx, secIdx, itemIdx, [end offsets]]
    postings: dict[str, list] = defaultdict(list)   # stem -> [pid, tf, pid, tf...] (raw pids)
    pid = 0
    total_chars = 0

    for ri, f in enumerate(files):
        d = json.loads(f.read_text(encoding="utf-8"))
        names = d.get("names", {})
        trad = d.get("traditional", {}) or {}
        p0 = pid
        for path, ii, text in iter_texts(trad):
            if path not in sections:
                sections[path] = len(sections)
            spans = split_passages(text)
            if not spans:
                continue
            ends = []
            # Store start of first passage + each end; starts of later passages = previous end
            # (gaps between passages are whitespace/bullets we skipped, so store explicit pairs
            # as [start0, end0, start1, end1 ...] deltas to stay exact)
            flat = []
            prev = 0
            for a, b in spans:
                flat.append(a - prev)
                flat.append(b - a)
                prev = b
                seg = text[a:b] if u16len(text) == len(text) else None
                if seg is None:
                    # text has astral chars; recompute via utf-16 decoding
                    buf = text.encode("utf-16-le")
                    seg = buf[a * 2:b * 2].decode("utf-16-le", errors="ignore")
                total_chars += len(seg)
                tf: dict[str, int] = defaultdict(int)
                for tok in tokenize(seg):
                    tf[tok] += 1
                for tok, c in tf.items():
                    postings[tok].append(pid)
                    postings[tok].append(c)
                pid += 1
            items.append([ri, sections[path], ii, flat])
        remedies.append({
            "id": d["id"],
            "primary": names.get("primary", ""),
            "latin": names.get("latin", ""),
            "common": names.get("common", []),
            "synonyms": names.get("synonyms", []),
            "category": d.get("category", ""),
            "n_sources": len((d.get("provenance") or {}).get("sources", [])),
            "has_traditional": bool(trad),
            "has_evidence": bool(d.get("evidence")),
            "p0": p0,
            "np": pid - p0,
        })
        if ri % 500 == 0:
            print(f"  {ri}/{len(files)}  passages so far: {pid}")

    n = pid
    print(f"{n} passages, {total_chars} chars, {len(postings)} stems")

    # Drop near-universal stems
    auto_stop = sorted(t for t, pl in postings.items() if len(pl) // 2 > n * AUTO_STOP_FRACTION)
    for t in auto_stop:
        del postings[t]
    print(f"auto-stop ({len(auto_stop)}): {auto_stop}")

    # Shard
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for old in OUT_DIR.glob("*.json"):
        old.unlink()
    by2: dict[str, dict[str, list]] = defaultdict(dict)
    for t, pl in postings.items():
        # delta-encode pids
        enc = []
        prev = 0
        for i in range(0, len(pl), 2):
            enc.append(pl[i] - prev)
            enc.append(pl[i + 1])
            prev = pl[i]
        by2[t[:2]][t] = enc

    shards3 = []
    written = 0
    for pre, terms in by2.items():
        blob = json.dumps(terms, separators=(",", ":"))
        if len(blob) > SHARD_SPLIT_BYTES:
            by3: dict[str, dict[str, list]] = defaultdict(dict)
            for t, enc in terms.items():
                by3[t[:3] if len(t) >= 3 else t][t] = enc
            for pre3, terms3 in by3.items():
                shards3.append(pre3)
                (OUT_DIR / f"{pre3}.json").write_text(
                    json.dumps(terms3, separators=(",", ":")), encoding="utf-8")
                written += 1
        else:
            (OUT_DIR / f"{pre}.json").write_text(blob, encoding="utf-8")
            written += 1

    sec_list = [None] * len(sections)
    for k, v in sections.items():
        sec_list[v] = k
    meta = {
        "n": n,
        "avg_chars": total_chars / max(n, 1),
        "remedies": remedies,
        "sections": sec_list,
        "non_symptom_sections": sorted(NON_SYMPTOM_SECTIONS),
        "items": items,
        "auto_stop": auto_stop,
        "shards3": sorted(shards3),
    }
    (OUT_DIR / "_meta.json").write_text(json.dumps(meta, separators=(",", ":"), ensure_ascii=False),
                                        encoding="utf-8")
    total = sum(p.stat().st_size for p in OUT_DIR.glob("*.json"))
    print(f"Wrote {written} shards + _meta.json ({(OUT_DIR / '_meta.json').stat().st_size / 1e6:.1f} MB); "
          f"total {total / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
