"""
Local web server for searching the homeopathy/natural medicine database.

Run:  python server.py
Open: http://localhost:8080

Requires: anthropic package (pip install anthropic) for the AI intake agent.
Set ANTHROPIC_API_KEY in .env or as an environment variable.
"""
from __future__ import annotations

import json
import os
import re
import ssl
import sys
import traceback
import urllib.parse
import urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MERGED_DIR = ROOT / "data" / "merged"
INDEX_PATH = ROOT / "data" / "search_index.json"

# Load .env if present
_env_path = ROOT / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
STATIC_DIR = ROOT / "static"

# Load search index at startup
print("Loading search index...")
search_index: list[dict] = json.loads(INDEX_PATH.read_text(encoding="utf-8"))
print(f"  {len(search_index)} remedies loaded")

# Build an inverted index for faster keyword search
print("Building inverted index...")
inverted: dict[str, set[int]] = {}
for i, entry in enumerate(search_index):
    # Index all searchable text: names, synonyms, snippet values
    texts = [
        entry["primary"].lower(),
        entry["latin"].lower(),
        " ".join(entry.get("common", [])).lower(),
        " ".join(entry.get("synonyms", [])).lower(),
    ]
    for v in entry.get("snippets", {}).values():
        texts.append(v.lower())
    combined = " ".join(texts)
    # Tokenize into words (3+ chars)
    tokens = set(re.findall(r"[a-z]{3,}", combined))
    for token in tokens:
        inverted.setdefault(token, set()).add(i)
print(f"  {len(inverted)} unique tokens indexed")


def search(query: str, category: str = "", dual_only: bool = False,
           limit: int = 100) -> list[dict]:
    """Search remedies by keyword query. Returns matching entries."""
    query = query.lower().strip()
    if not query:
        results = list(range(len(search_index)))
    else:
        words = re.findall(r"[a-z]{3,}", query)
        if not words:
            return []
        # Intersect matches for all query words (AND logic)
        result_sets = []
        for word in words:
            matches = set()
            for token, indices in inverted.items():
                if word in token:  # substring match
                    matches |= indices
            result_sets.append(matches)
        if not result_sets:
            return []
        results = sorted(result_sets[0].intersection(*result_sets[1:]))

    # Filter
    out = []
    for idx in results:
        entry = search_index[idx]
        if category and entry["category"] != category:
            continue
        if dual_only and not (entry["has_traditional"] and entry["has_evidence"]):
            continue
        # Compute a simple relevance score
        score = entry["n_sources"]
        name_lower = entry["primary"].lower() + " " + entry["latin"].lower()
        if query and query in name_lower:
            score += 100  # boost exact name matches
        out.append({**entry, "_score": score})
        if len(out) >= limit * 3:  # pre-limit for sorting
            break

    out.sort(key=lambda r: -r["_score"])
    return out[:limit]


def get_remedy_detail(remedy_id: str) -> dict | None:
    """Load full remedy data from merged JSON."""
    path = MERGED_DIR / f"{remedy_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


# ---------- Intake matching ----------

# Map form selections to homeopathic search terms
KEYWORD_EXPANSIONS: dict[str, list[str]] = {
    # Sensations
    "Aching / Dull": ["aching", "dull", "pain"],
    "Boring / Drilling": ["boring", "drilling"],
    "Bruised / Sore": ["bruised", "sore", "lame"],
    "Burning": ["burning", "burn", "heat"],
    "Bursting": ["bursting", "fullness"],
    "Constricting / Band-like": ["constricting", "constriction", "band"],
    "Cramping": ["cramping", "cramp", "spasm"],
    "Crushing / Heavy": ["crushing", "heavy", "heaviness", "weight"],
    "Cutting": ["cutting", "cut"],
    "Drawing / Pulling": ["drawing", "pulling", "tearing"],
    "Gnawing": ["gnawing"],
    "Itching": ["itching", "itch", "pruritus"],
    "Lancinating / Piercing": ["lancinating", "piercing"],
    "Numb / Tingling": ["numbness", "tingling", "numb"],
    "Pressing": ["pressing", "pressure"],
    "Pulsating / Throbbing": ["pulsating", "throbbing", "pulsation"],
    "Raw / Excoriated": ["raw", "excoriated", "soreness"],
    "Sharp": ["sharp", "acute"],
    "Shooting": ["shooting", "radiating"],
    "Splinter-like": ["splinter"],
    "Stitching / Stabbing": ["stitching", "stabbing", "stitch"],
    "Stinging": ["stinging", "sting"],
    "Tearing": ["tearing", "tear"],
    "Wandering / Moving": ["wandering", "shifting", "moving"],
    # Thermal
    "Chilly / Cold person - prefers warmth": ["chilly", "cold", "warmth", "amelioration"],
    "Warm / Hot person - prefers cool": ["warm", "hot", "cool"],
    # Thirst
    "Very thirsty - large quantities": ["thirst", "large quantities"],
    "Thirsty - small sips frequently": ["thirst", "sips"],
    "Thirstless / Low thirst": ["thirstless"],
    "Thirst for ice-cold water": ["thirst", "cold water", "ice"],
    "Thirst for warm drinks": ["thirst", "warm drinks"],
    # Sleep
    "Restless": ["restless", "restlessness"],
    "Unrefreshing": ["unrefreshing", "sleep"],
    "Difficulty falling asleep": ["insomnia", "sleeplessness"],
    "Early waking (3-5am)": ["waking", "early morning"],
    "Sleepless after midnight": ["sleepless", "midnight"],
    "Profuse / Excessive": ["perspiration", "profuse", "sweat"],
    "Cold sweat": ["cold sweat", "perspiration"],
    "Night sweats": ["night sweats", "perspiration"],
}

# Body location to repertory terms
LOCATION_TERMS: dict[str, list[str]] = {
    "Head - Forehead": ["forehead", "head", "frontal"],
    "Head - Temples": ["temples", "head", "temporal"],
    "Head - Vertex (top)": ["vertex", "head", "top"],
    "Head - Occiput (back)": ["occiput", "head", "occipital"],
    "Head - Sides": ["head", "sides"],
    "Head - Whole head": ["head", "headache", "cephalalgia"],
    "Eyes": ["eyes", "vision", "ocular"],
    "Ears": ["ears", "hearing", "tinnitus"],
    "Nose": ["nose", "nasal", "coryza"],
    "Face - Cheeks": ["face", "cheeks"],
    "Face - Jaw": ["face", "jaw"],
    "Mouth": ["mouth", "oral"],
    "Teeth": ["teeth", "dental", "toothache"],
    "Tongue": ["tongue"],
    "Throat": ["throat", "pharynx", "tonsils"],
    "Neck - Cervical": ["neck", "cervical"],
    "Back - Upper (thoracic)": ["back", "thoracic", "dorsal"],
    "Back - Between scapulae": ["back", "scapulae", "between shoulders"],
    "Back - Lower (lumbar)": ["back", "lumbar", "lumbago"],
    "Back - Sacral": ["back", "sacrum", "sacral"],
    "Back - Coccyx": ["coccyx", "tailbone"],
    "Chest - General": ["chest"],
    "Chest - Heart region": ["heart", "chest", "cardiac", "palpitation"],
    "Chest - Lungs": ["lungs", "chest", "pulmonary"],
    "Chest - Breasts": ["breast", "mammary"],
    "Abdomen - Upper (epigastric)": ["abdomen", "epigastric", "stomach"],
    "Abdomen - Umbilical": ["abdomen", "umbilical", "navel"],
    "Abdomen - Lower (hypogastric)": ["abdomen", "hypogastric", "lower abdomen"],
    "Abdomen - Right side": ["abdomen", "right side", "liver"],
    "Abdomen - Left side": ["abdomen", "left side", "spleen"],
    "Abdomen - Liver region": ["liver", "hepatic", "abdomen"],
    "Stomach": ["stomach", "gastric", "digestion", "nausea"],
    "Rectum": ["rectum", "anus", "hemorrhoids"],
    "Bladder": ["bladder", "urinary", "cystitis"],
    "Kidneys": ["kidneys", "renal"],
    "Urethra": ["urethra", "urinary"],
    "Shoulder - Right": ["shoulder", "right"],
    "Shoulder - Left": ["shoulder", "left"],
    "Elbow": ["elbow"],
    "Wrist": ["wrist"],
    "Hand": ["hand", "hands"],
    "Fingers": ["fingers"],
    "Hip": ["hip"],
    "Thigh": ["thigh"],
    "Knee": ["knee"],
    "Ankle": ["ankle"],
    "Foot": ["foot", "feet"],
    "Toes": ["toes"],
    "Skin": ["skin", "eruptions", "dermatitis"],
    "Joints - General": ["joints", "arthritis", "rheumatic"],
    "Muscles - General": ["muscles", "muscular"],
    "Bones": ["bones", "periosteum"],
    "Whole body / General": ["general", "weakness", "fatigue"],
}


# ---------- AI Agent (Claude API via urllib) ----------

REPERTORIZATION_PROMPT = """You are an expert homeopathic repertorization agent. You have deep knowledge of:
- Kent's Repertory structure (Mind, Head, Eyes, Ears, Nose, Face, Mouth, Throat, Stomach, Abdomen, Rectum, Urinary, Genitalia, Larynx, Chest, Back, Extremities, Sleep, Fever, Perspiration, Skin, Generalities)
- Homeopathic modalities (ameliorations and aggravations)
- Constitutional prescribing (thermal state, food desires/aversions, thirst, body type)
- Miasmatic theory
- Cell salts (Schuessler tissue salts #1-12)
- The difference between "running" as exercise/modality vs "running" as a discharge quality

Your job is to analyze a patient intake form and produce PRECISE search queries for a homeopathic database. You must understand CONTEXT — for example:
- "knee pain worse from running" → search for knee + pain + aggravation motion, NOT "running nose"
- "burning in stomach better cold drinks" → search for stomach + burning + amelioration cold drinks
- "worse after sleeping" → this is a classic Lachesis modality (aggravation after sleep)
- "desires salt" → constitutional symptom pointing to Natrum Muriaticum

Given the patient intake data below, produce a JSON response with this exact structure:
{
  "analysis": "Brief 2-3 sentence summary of the case in homeopathic terms",
  "search_groups": [
    {
      "label": "Human-readable description of what this search targets",
      "required_terms": ["words that MUST appear together in a remedy"],
      "bonus_terms": ["additional words that boost score if found"],
      "weight": 1.0 to 5.0 (importance: 5=keynote/striking, 3=common, 1=general),
      "repertory_section": "which Kent repertory chapter this belongs to"
    }
  ],
  "suggested_remedies": ["Up to 10 remedy names you think are most likely based on the totality of symptoms — use Latin names"],
  "reasoning": "Brief explanation of why you selected those remedies"
}

RULES:
1. Create 5-20 search groups covering the most important symptoms
2. Put the MOST CHARACTERISTIC/PECULIAR symptoms at highest weight
3. Modalities (better/worse) are very important — weight them 3-5
4. Mental/emotional symptoms are often the most important — weight them 4-5
5. Use proper homeopathic terminology in required_terms (e.g., "aggravation" not just "worse")
6. For body locations, use both common and Latin terms
7. Group related concepts: don't search "running" alone, search ["knee", "pain", "motion", "aggravation"] together
8. Your suggested_remedies should be based on the TOTALITY, not individual symptoms
9. Return ONLY valid JSON, no markdown formatting"""


def call_claude_agent(payload: dict) -> dict | None:
    """Call Claude API to interpret intake form data for repertorization.
    Uses urllib so no pip install needed. Returns parsed JSON or None."""
    if not ANTHROPIC_API_KEY:
        return None

    # Build a readable summary of the intake form
    lines = []
    # Symptoms
    sx_indices: set[int] = set()
    for key in payload:
        m = re.match(r"sx_\w+_(\d+)$", key)
        if m:
            sx_indices.add(int(m.group(1)))

    for si in sorted(sx_indices):
        desc = payload.get(f"sx_desc_{si}", "")
        loc = payload.get(f"sx_location_{si}", "")
        sens = payload.get(f"sx_sensation_{si}", "")
        side = payload.get(f"sx_side_{si}", "")
        better = payload.get(f"sx_better_{si}", [])
        worse = payload.get(f"sx_worse_{si}", [])
        if isinstance(better, str): better = [better]
        if isinstance(worse, str): worse = [worse]
        lines.append(f"SYMPTOM {si + 1}:")
        if desc: lines.append(f"  Description: {desc}")
        if loc: lines.append(f"  Location: {loc}")
        if sens: lines.append(f"  Sensation: {sens}")
        if side and side != "N/A": lines.append(f"  Side: {side}")
        if better: lines.append(f"  Better from: {', '.join(better)}")
        if worse: lines.append(f"  Worse from: {', '.join(worse)}")

    # Constitutional
    for field, label in [("thermal", "Temperature"), ("thirst", "Thirst"),
                          ("build", "Body Build"), ("appetite", "Appetite")]:
        val = payload.get(field)
        if val: lines.append(f"{label}: {val}")

    # Ailments from
    af = payload.get("ailments_from")
    if af: lines.append(f"Ailments From / Causation: {af}")

    # Checked items
    checked = payload.get("_checked", [])
    if isinstance(checked, str): checked = [checked]
    if checked:
        # Group by prefix
        emotions = [c for c in checked if any(e in c.lower() for e in
                    ["anxiety", "irritab", "anger", "sadness", "weeping", "restless",
                     "apathy", "fear", "mood", "impatien", "jealous", "guilt",
                     "despair", "sensitiv", "solitude", "company", "excit",
                     "confus", "concentrat", "forgetful", "suspic", "obstin"])]
        fears = [c for c in checked if c.startswith(("Death", "Disease", "Being alone",
                 "Dark", "Height", "Crowd", "Narrow", "Thunder", "Animal", "Insect",
                 "Future", "Failure", "Going insane", "Poverty", "Robber", "Cancer",
                 "Heart disease", "Needle", "Flying", "Water", "Ghost", "Something bad",
                 "Losing control", "Public"))]
        food_desires = [c for c in checked if c in
                       {"Salt/salty", "Sweets/sugar", "Sour/acids", "Spicy/hot",
                        "Bitter", "Fat/rich food", "Milk", "Cheese", "Eggs", "Bread",
                        "Meat", "Fish", "Chocolate", "Coffee", "Tea", "Alcohol/wine",
                        "Cold drinks", "Warm drinks", "Ice/ice cream", "Fruit",
                        "Vegetables", "Raw food", "Smoked food", "Pickles",
                        "Oysters", "Onions", "Garlic", "Potatoes", "Rice", "Butter"}]
        food_aversions = [c for c in checked if c in
                         {"Meat", "Fat/rich food", "Milk", "Eggs", "Bread", "Coffee",
                          "Salt", "Sweets", "Fish", "Fruit", "Vegetables", "Butter",
                          "Cheese", "Alcohol", "Warm food", "Cold food", "Cooked food",
                          "All food"} and c not in food_desires]
        systems = [c for c in checked if c not in emotions and c not in fears
                   and c not in food_desires and c not in food_aversions]

        if emotions: lines.append(f"Emotional State: {', '.join(emotions)}")
        if fears: lines.append(f"Fears: {', '.join(fears)}")
        if food_desires: lines.append(f"Food Desires: {', '.join(food_desires)}")
        if food_aversions: lines.append(f"Food Aversions: {', '.join(food_aversions)}")
        if systems: lines.append(f"Systems Review: {', '.join(systems)}")

    # Sleep
    for field, label in [("sleep_position", "Sleep Position"),
                          ("sleep_quality", "Sleep Quality"),
                          ("sweat_pattern", "Perspiration"),
                          ("sweat_location", "Perspiration Location")]:
        val = payload.get(field)
        if val: lines.append(f"{label}: {val}")

    # Free text
    for field, label in [("past_medical", "Past Medical History"),
                          ("family_history", "Family History"),
                          ("medications", "Current Medications"),
                          ("additional", "Additional Notes")]:
        val = payload.get(field)
        if val and isinstance(val, str) and val.strip():
            lines.append(f"{label}: {val}")

    patient_summary = "\n".join(lines)

    # Call Claude API
    api_body = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 2000,
        "messages": [
            {
                "role": "user",
                "content": f"{REPERTORIZATION_PROMPT}\n\n--- PATIENT INTAKE DATA ---\n{patient_summary}"
            }
        ]
    })

    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=api_body.encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        # Extract text from Claude response
        text = ""
        for block in result.get("content", []):
            if block.get("type") == "text":
                text += block["text"]
        # Parse JSON from response (handle markdown code blocks)
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        return json.loads(text)
    except Exception as e:
        print(f"  [!] Claude API error: {e}", flush=True)
        traceback.print_exc()
        return None


def agent_intake_match(payload: dict) -> dict:
    """AI-powered intake matching using Claude for repertorization."""
    # Step 1: Ask Claude to analyze the intake
    agent_result = call_claude_agent(payload)

    if not agent_result:
        # Fallback to keyword matching if API unavailable
        print("  [!] AI agent unavailable, falling back to keyword matching",
              flush=True)
        return intake_match(payload)

    scores: dict[int, float] = {}
    matched_kw: dict[int, list[str]] = {}
    search_phrases_used: list[str] = []

    def award(idx: int, points: float, label: str):
        scores[idx] = scores.get(idx, 0) + points
        matched_kw.setdefault(idx, [])
        if label not in matched_kw[idx]:
            matched_kw[idx].append(label)

    # Step 2: Execute the search groups from Claude's analysis
    search_groups = agent_result.get("search_groups", [])
    for group in search_groups:
        label = group.get("label", "")
        required = group.get("required_terms", [])
        bonus = group.get("bonus_terms", [])
        weight = float(group.get("weight", 2.0))

        if not required:
            continue

        search_phrases_used.append(label)

        # Find remedies that match ALL required terms (AND logic)
        required_lower = [t.lower() for t in required if len(t) >= 3]
        if not required_lower:
            continue

        # Get hits for each required term
        term_hits = []
        for term in required_lower:
            hits: set[int] = set()
            for token, indices in inverted.items():
                if term in token:
                    hits |= indices
            term_hits.append(hits)

        # Intersect — must match ALL required terms
        if term_hits:
            combined = term_hits[0]
            for th in term_hits[1:]:
                combined &= th
            for idx in combined:
                award(idx, weight, label)

        # Bonus terms add extra weight to already-matching remedies
        bonus_lower = [t.lower() for t in bonus if len(t) >= 3]
        for bt in bonus_lower:
            bt_hits: set[int] = set()
            for token, indices in inverted.items():
                if bt in token:
                    bt_hits |= indices
            for idx in bt_hits:
                if idx in scores:  # Only boost if already matched
                    award(idx, weight * 0.3, f"+ {bt}")

    # Step 3: Boost Claude's suggested remedies
    suggested = agent_result.get("suggested_remedies", [])
    for remedy_name in suggested:
        rn_lower = remedy_name.lower()
        for i, entry in enumerate(search_index):
            name_text = (entry["primary"] + " " + entry["latin"] + " " +
                        " ".join(entry.get("common", [])) + " " +
                        " ".join(entry.get("synonyms", []))).lower()
            if rn_lower in name_text or any(
                w in name_text for w in rn_lower.split()
                if len(w) >= 4
            ):
                award(i, 8.0, f"AI suggested: {remedy_name}")
                break

    if not scores:
        # If agent produced no usable results, fall back
        return intake_match(payload)

    # Sort by score
    ranked = sorted(scores.items(), key=lambda x: -x[1])

    results = []
    for idx, score in ranked[:50]:
        e = search_index[idx]
        results.append({
            "id": e["id"],
            "primary": e["primary"],
            "latin": e["latin"],
            "category": e["category"],
            "n_sources": e["n_sources"],
            "has_traditional": e["has_traditional"],
            "has_evidence": e["has_evidence"],
            "score": round(score, 1),
            "matched_keywords": matched_kw.get(idx, [])[:20],
        })

    return {
        "keywords_used": len(set(search_phrases_used)),
        "ai_analysis": agent_result.get("analysis", ""),
        "ai_reasoning": agent_result.get("reasoning", ""),
        "results": results,
    }


def _snippet_text_for(idx: int) -> str:
    """Return the full combined snippet text for a remedy (cached)."""
    e = search_index[idx]
    parts = [
        e["primary"].lower(),
        e["latin"].lower(),
        " ".join(e.get("common", [])).lower(),
        " ".join(e.get("synonyms", [])).lower(),
    ]
    for v in e.get("snippets", {}).values():
        parts.append(v.lower())
    return " ".join(parts)


# Build a per-remedy field index so we can search within specific
# repertory sections (head, knee, modalities, etc.)
print("Building field-level index...")
field_index: dict[int, dict[str, str]] = {}
for i, entry in enumerate(search_index):
    fields: dict[str, str] = {}
    for k, v in entry.get("snippets", {}).items():
        fields[k.lower()] = v.lower()
    # Also index names
    fields["_names"] = " ".join([
        entry["primary"].lower(),
        entry["latin"].lower(),
        " ".join(entry.get("common", [])).lower(),
        " ".join(entry.get("synonyms", [])).lower(),
    ])
    field_index[i] = fields
print(f"  {len(field_index)} remedy field maps built")

# Stop words — never search these alone
STOP_WORDS = {
    "the", "and", "for", "with", "from", "that", "this", "are",
    "not", "but", "was", "has", "had", "have", "been", "were",
    "also", "more", "very", "than", "all", "can", "its", "will",
    "may", "after", "before", "during", "about", "into", "over",
    "such", "each", "which", "their", "other", "being", "gets",
    "like", "side", "does", "one", "two", "who", "when", "some",
    "there", "then", "only", "just", "much", "most", "even",
    "well", "use", "used", "often", "upon", "many",
}


def _phrase_in_remedy(phrase: str, idx: int,
                      restrict_fields: list[str] | None = None) -> bool:
    """Check if a multi-word phrase appears in a remedy's text.
    If restrict_fields is given, only search within snippet fields
    whose key contains one of those strings."""
    fields = field_index.get(idx, {})
    for fkey, ftext in fields.items():
        if restrict_fields:
            if not any(r in fkey for r in restrict_fields):
                continue
        if phrase in ftext:
            return True
    return False


def _token_hits(token: str) -> set[int]:
    """Get all remedy indices that contain this token (substring match)."""
    hits: set[int] = set()
    for inv_token, indices in inverted.items():
        if token in inv_token:
            hits |= indices
    return hits


def intake_match(payload: dict) -> dict:
    """Context-aware intake form matching.

    Instead of searching each keyword independently, this groups symptom
    data by context (e.g. body location + sensation + modality for each
    symptom entry) and uses phrase/co-occurrence matching so that
    'knee pain worse from running' searches for remedies that mention
    knee AND pain together, not 'running nose'.
    """
    scores: dict[int, float] = {}
    matched_kw: dict[int, list[str]] = {}
    search_phrases_used: list[str] = []

    def award(idx: int, points: float, label: str):
        scores[idx] = scores.get(idx, 0) + points
        matched_kw.setdefault(idx, [])
        if label not in matched_kw[idx]:
            matched_kw[idx].append(label)

    # ---- 1. SYMPTOM ENTRIES (context-grouped) ----
    # Collect symptom entries by index number
    symptom_indices: set[int] = set()
    for key in payload:
        m = re.match(r"sx_\w+_(\d+)$", key)
        if m:
            symptom_indices.add(int(m.group(1)))

    for sx_idx in sorted(symptom_indices):
        desc = payload.get(f"sx_desc_{sx_idx}", "") or ""
        location = payload.get(f"sx_location_{sx_idx}", "") or ""
        sensation = payload.get(f"sx_sensation_{sx_idx}", "") or ""
        side_val = payload.get(f"sx_side_{sx_idx}", "") or ""
        better = payload.get(f"sx_better_{sx_idx}", [])
        worse = payload.get(f"sx_worse_{sx_idx}", [])
        if isinstance(better, str):
            better = [better]
        if isinstance(worse, str):
            worse = [worse]

        # Get the body part keywords for this symptom
        loc_terms = LOCATION_TERMS.get(location, [])
        if not loc_terms and location:
            loc_terms = [w for w in re.findall(r"[a-z]{3,}", location.lower())
                         if w not in STOP_WORDS]

        # Get sensation keywords
        sens_terms = KEYWORD_EXPANSIONS.get(sensation, [])
        if not sens_terms and sensation:
            sens_terms = [w for w in re.findall(r"[a-z]{3,}", sensation.lower())
                          if w not in STOP_WORDS]

        # --- Context-aware matching: location + sensation together ---
        # Find remedies that mention BOTH the body part AND the sensation
        if loc_terms and sens_terms:
            loc_hits: set[int] = set()
            for lt in loc_terms:
                loc_hits |= _token_hits(lt)
            sens_hits: set[int] = set()
            for st in sens_terms:
                sens_hits |= _token_hits(st)
            # Remedies matching BOTH get a big bonus
            combo_hits = loc_hits & sens_hits
            combo_label = f"{location} + {sensation}"
            search_phrases_used.append(combo_label)
            for idx in combo_hits:
                award(idx, 5.0, combo_label)
            # Remedies matching just location or just sensation get less
            for idx in loc_hits - combo_hits:
                award(idx, 1.0, location)
            for idx in sens_hits - combo_hits:
                award(idx, 1.0, sensation)
        elif loc_terms:
            label = location or loc_terms[0]
            search_phrases_used.append(label)
            for lt in loc_terms:
                for idx in _token_hits(lt):
                    award(idx, 2.0, label)
        elif sens_terms:
            label = sensation or sens_terms[0]
            search_phrases_used.append(label)
            for st in sens_terms:
                for idx in _token_hits(st):
                    award(idx, 2.0, label)

        # --- Side ---
        if side_val and side_val not in ("N/A", "Both sides"):
            side_words = [w for w in re.findall(r"[a-z]{3,}", side_val.lower())
                          if w not in STOP_WORDS]
            # Only score side if it co-occurs with the body location
            if loc_terms and side_words:
                loc_hits_set = set()
                for lt in loc_terms:
                    loc_hits_set |= _token_hits(lt)
                for sw in side_words:
                    side_hits = _token_hits(sw) & loc_hits_set
                    label = f"{side_val} {location}"
                    search_phrases_used.append(label)
                    for idx in side_hits:
                        award(idx, 1.5, label)

        # --- Modalities: search as phrases within modality-relevant fields ---
        for mod_list, mod_type in [(better, "better"), (worse, "worse")]:
            for mod_item in mod_list:
                if not mod_item:
                    continue
                mod_words = [w for w in re.findall(r"[a-z]{3,}", mod_item.lower())
                             if w not in STOP_WORDS]
                if not mod_words:
                    continue
                # Search for modality co-occurring with body location
                mod_hits: set[int] = set()
                for mw in mod_words:
                    mod_hits |= _token_hits(mw)
                # If we have a location, prefer remedies that mention both
                if loc_terms:
                    loc_set = set()
                    for lt in loc_terms:
                        loc_set |= _token_hits(lt)
                    contextual = mod_hits & loc_set
                    label = f"{mod_type}: {mod_item}"
                    search_phrases_used.append(label)
                    for idx in contextual:
                        award(idx, 2.5, label)
                    # Pure modality match (without body part) gets less
                    for idx in mod_hits - contextual:
                        award(idx, 0.5, label)
                else:
                    label = f"{mod_type}: {mod_item}"
                    search_phrases_used.append(label)
                    for idx in mod_hits:
                        award(idx, 1.5, label)

        # --- Free-text description: extract meaningful phrases ---
        if desc:
            desc_lower = desc.lower()
            desc_words = [w for w in re.findall(r"[a-z]{3,}", desc_lower)
                          if w not in STOP_WORDS]
            # Search for the full description as a phrase first
            if len(desc_words) >= 2:
                # Try 2-word and 3-word phrase windows
                for window in (3, 2):
                    for i in range(len(desc_words) - window + 1):
                        phrase = " ".join(desc_words[i:i + window])
                        phrase_hits = set()
                        # All words must be present
                        word_sets = [_token_hits(w) for w in desc_words[i:i + window]]
                        if word_sets:
                            phrase_hits = word_sets[0]
                            for ws in word_sets[1:]:
                                phrase_hits &= ws
                        if phrase_hits:
                            search_phrases_used.append(phrase)
                            for idx in phrase_hits:
                                award(idx, 3.0, phrase)
            # Individual words from description at lower weight
            for w in desc_words:
                for idx in _token_hits(w):
                    award(idx, 0.5, w)

    # ---- 2. AILMENTS FROM (causation — very important) ----
    ailments = payload.get("ailments_from", "")
    if ailments:
        terms = [w for w in re.findall(r"[a-z]{3,}", ailments.lower())
                 if w not in STOP_WORDS]
        label = f"Ailments from: {ailments}"
        search_phrases_used.append(label)
        if len(terms) >= 2:
            # All words must co-occur
            hits = _token_hits(terms[0])
            for t in terms[1:]:
                hits &= _token_hits(t)
            for idx in hits:
                award(idx, 4.0, label)
        elif terms:
            for idx in _token_hits(terms[0]):
                award(idx, 3.0, label)

    # ---- 3. CONSTITUTIONAL (thermal, thirst, food, etc.) ----
    for field in ["thermal", "thirst", "build", "appetite"]:
        val = payload.get(field)
        if not val:
            continue
        expanded = KEYWORD_EXPANSIONS.get(val, [])
        terms = expanded if expanded else [w for w in re.findall(r"[a-z]{3,}", val.lower())
                                            if w not in STOP_WORDS]
        if not terms:
            continue
        label = val
        search_phrases_used.append(label)
        # Constitutional matches — find remedies mentioning these terms
        if len(terms) >= 2:
            hits = _token_hits(terms[0])
            for t in terms[1:]:
                hits &= _token_hits(t)
            for idx in hits:
                award(idx, 2.0, label)
        else:
            for idx in _token_hits(terms[0]):
                award(idx, 1.5, label)

    # ---- 4. SLEEP / PERSPIRATION ----
    for field in ["sleep_position", "sleep_quality", "sweat_pattern",
                   "sweat_location"]:
        val = payload.get(field)
        if not val:
            continue
        expanded = KEYWORD_EXPANSIONS.get(val, [])
        terms = expanded if expanded else [w for w in re.findall(r"[a-z]{3,}", val.lower())
                                            if w not in STOP_WORDS]
        if not terms:
            continue
        label = val
        search_phrases_used.append(label)
        for t in terms:
            for idx in _token_hits(t):
                award(idx, 1.5, label)

    # ---- 5. CHECKED ITEMS (emotions, fears, food, systems) ----
    checked = payload.get("_checked", [])
    if isinstance(checked, str):
        checked = [checked]
    for item in checked:
        terms = [w for w in re.findall(r"[a-z]{3,}", item.lower())
                 if w not in STOP_WORDS]
        if not terms:
            continue
        label = item
        search_phrases_used.append(label)
        if len(terms) >= 2:
            # Phrase match — all words must co-occur
            hits = _token_hits(terms[0])
            for t in terms[1:]:
                hits &= _token_hits(t)
            for idx in hits:
                award(idx, 2.0, label)
        else:
            for idx in _token_hits(terms[0]):
                award(idx, 1.5, label)

    # ---- 6. FREE-TEXT FIELDS (low weight) ----
    for field in ["past_medical", "family_history", "medications", "additional"]:
        val = payload.get(field)
        if not val or not isinstance(val, str):
            continue
        words = [w for w in re.findall(r"[a-z]{3,}", val.lower())
                 if w not in STOP_WORDS]
        for w in words:
            for idx in _token_hits(w):
                award(idx, 0.3, w)

    if not scores:
        return {"keywords_used": 0, "results": []}

    # Sort by score descending
    ranked = sorted(scores.items(), key=lambda x: -x[1])

    # Return top 50
    results = []
    for idx, score in ranked[:50]:
        e = search_index[idx]
        results.append({
            "id": e["id"],
            "primary": e["primary"],
            "latin": e["latin"],
            "category": e["category"],
            "n_sources": e["n_sources"],
            "has_traditional": e["has_traditional"],
            "has_evidence": e["has_evidence"],
            "score": round(score, 1),
            "matched_keywords": matched_kw.get(idx, [])[:20],
        })

    return {
        "keywords_used": len(set(search_phrases_used)),
        "results": results,
    }


# HTML template
HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Homeopathy & Natural Medicine Database</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #0f1419;
    color: #e7e9ea;
    min-height: 100vh;
}
.header {
    background: linear-gradient(135deg, #1a3a2a 0%, #0f1f2f 100%);
    border-bottom: 1px solid #2f3336;
    padding: 20px;
    text-align: center;
}
.header h1 {
    font-size: 1.5rem;
    color: #7ec8a0;
    margin-bottom: 4px;
}
.header .subtitle {
    color: #71767b;
    font-size: 0.85rem;
}
.search-bar {
    max-width: 800px;
    margin: 20px auto;
    padding: 0 16px;
}
.search-input {
    width: 100%;
    padding: 14px 18px;
    border: 1px solid #2f3336;
    border-radius: 12px;
    background: #16202a;
    color: #e7e9ea;
    font-size: 1rem;
    outline: none;
    transition: border-color 0.2s;
}
.search-input:focus { border-color: #7ec8a0; }
.search-input::placeholder { color: #71767b; }
.filters {
    max-width: 800px;
    margin: 10px auto;
    padding: 0 16px;
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    align-items: center;
}
.filters label { color: #71767b; font-size: 0.85rem; }
.filters select, .filters input[type="checkbox"] {
    background: #16202a;
    color: #e7e9ea;
    border: 1px solid #2f3336;
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 0.85rem;
}
.stats {
    max-width: 800px;
    margin: 8px auto;
    padding: 0 16px;
    color: #71767b;
    font-size: 0.8rem;
}
.results {
    max-width: 800px;
    margin: 10px auto;
    padding: 0 16px;
}
.result-card {
    background: #16202a;
    border: 1px solid #2f3336;
    border-radius: 12px;
    padding: 16px;
    margin-bottom: 10px;
    cursor: pointer;
    transition: border-color 0.2s, background 0.2s;
}
.result-card:hover {
    border-color: #7ec8a0;
    background: #1a2a34;
}
.result-name {
    font-size: 1.1rem;
    font-weight: 600;
    color: #e7e9ea;
}
.result-latin {
    color: #7ec8a0;
    font-style: italic;
    font-size: 0.9rem;
}
.result-meta {
    display: flex;
    gap: 12px;
    margin-top: 6px;
    flex-wrap: wrap;
}
.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 500;
}
.badge-cat {
    background: #1d3a2d;
    color: #7ec8a0;
}
.badge-sources {
    background: #1d2d3a;
    color: #6bb3d9;
}
.badge-trad {
    background: #3a2d1d;
    color: #d9a86b;
}
.badge-ev {
    background: #2d1d3a;
    color: #b36bd9;
}
.result-snippet {
    margin-top: 8px;
    color: #8b8f94;
    font-size: 0.85rem;
    line-height: 1.4;
    max-height: 60px;
    overflow: hidden;
}

/* Detail modal */
.modal-overlay {
    display: none;
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.7);
    z-index: 1000;
    overflow-y: auto;
}
.modal-overlay.active { display: block; }
.modal {
    max-width: 900px;
    margin: 30px auto;
    background: #16202a;
    border: 1px solid #2f3336;
    border-radius: 16px;
    padding: 24px;
    min-height: 200px;
}
.modal-close {
    float: right;
    background: none;
    border: none;
    color: #71767b;
    font-size: 1.5rem;
    cursor: pointer;
    padding: 4px 8px;
}
.modal-close:hover { color: #e7e9ea; }
.modal h2 {
    color: #7ec8a0;
    font-size: 1.4rem;
    margin-bottom: 4px;
}
.modal h3 {
    color: #6bb3d9;
    font-size: 1.1rem;
    margin: 16px 0 8px;
    border-bottom: 1px solid #2f3336;
    padding-bottom: 4px;
}
.modal h4 {
    color: #d9a86b;
    font-size: 0.95rem;
    margin: 12px 0 4px;
}
.modal .section-text {
    color: #b0b3b8;
    font-size: 0.88rem;
    line-height: 1.6;
    margin: 4px 0 8px;
    white-space: pre-wrap;
    max-height: 400px;
    overflow-y: auto;
}
.modal .names-block {
    color: #8b8f94;
    font-size: 0.85rem;
    margin-bottom: 12px;
}
.tab-bar {
    display: flex;
    gap: 0;
    margin: 16px 0 12px;
    border-bottom: 2px solid #2f3336;
}
.tab {
    padding: 8px 20px;
    cursor: pointer;
    color: #71767b;
    font-size: 0.9rem;
    border-bottom: 2px solid transparent;
    margin-bottom: -2px;
    transition: color 0.2s, border-color 0.2s;
}
.tab:hover { color: #e7e9ea; }
.tab.active {
    color: #7ec8a0;
    border-bottom-color: #7ec8a0;
}
.tab-content { display: none; }
.tab-content.active { display: block; }
.source-list {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 8px;
}
.source-tag {
    background: #1d2d3a;
    color: #6bb3d9;
    padding: 2px 8px;
    border-radius: 8px;
    font-size: 0.75rem;
}
</style>
</head>
<body>

<div class="header">
    <h1>Homeopathy & Natural Medicine Database</h1>
    <div class="subtitle">REMEDY_COUNT remedies from SOURCE_COUNT sources &mdash; search by symptom, remedy name, or body region</div>
    <div style="margin-top:8px"><a href="/intake" style="color:#6bb3d9;text-decoration:none;font-size:0.9rem">&#9654; Full Intake Assessment Form</a></div>
</div>

<div class="search-bar">
    <input type="text" class="search-input" id="searchInput"
           placeholder="Search symptoms, remedy names, body regions... (e.g. 'headache throbbing', 'arnica', 'liver toxicity')"
           autofocus>
</div>

<div class="filters">
    <label>Category:</label>
    <select id="filterCategory">
        <option value="">All</option>
        <option value="homeopathic">Homeopathic</option>
        <option value="herbal">Herbal</option>
        <option value="supplement">Supplement</option>
    </select>
    <label style="margin-left:10px">
        <input type="checkbox" id="filterDual"> Traditional + Evidence only
    </label>
</div>

<div class="stats" id="statsLine"></div>

<div class="results" id="results"></div>

<div class="modal-overlay" id="modalOverlay">
    <div class="modal" id="modalContent">
        <button class="modal-close" onclick="closeModal()">&times;</button>
        <div id="modalBody">Loading...</div>
    </div>
</div>

<script>
const searchInput = document.getElementById('searchInput');
const filterCategory = document.getElementById('filterCategory');
const filterDual = document.getElementById('filterDual');
const resultsDiv = document.getElementById('results');
const statsLine = document.getElementById('statsLine');
const modalOverlay = document.getElementById('modalOverlay');
const modalBody = document.getElementById('modalBody');

let debounceTimer;

function doSearch() {
    const q = searchInput.value.trim();
    const cat = filterCategory.value;
    const dual = filterDual.checked;
    const params = new URLSearchParams();
    if (q) params.set('q', q);
    if (cat) params.set('category', cat);
    if (dual) params.set('dual', '1');

    fetch('/api/search?' + params.toString())
        .then(r => r.json())
        .then(data => renderResults(data));
}

function renderResults(data) {
    statsLine.textContent = `${data.total} results` +
        (data.total > data.results.length ? ` (showing ${data.results.length})` : '');

    if (!data.results.length) {
        resultsDiv.innerHTML = '<div style="text-align:center;color:#71767b;padding:40px">No results found. Try different keywords.</div>';
        return;
    }

    resultsDiv.innerHTML = data.results.map(r => {
        const snipKeys = Object.keys(r.snippets || {});
        let snippet = '';
        if (snipKeys.length) {
            const key = snipKeys.find(k => k.includes('keynote') || k.includes('summary') || k.includes('indication')) || snipKeys[0];
            snippet = (r.snippets[key] || '').substring(0, 200);
        }
        const common = (r.common || []).slice(0, 3).join(', ');
        return `
        <div class="result-card" onclick="showDetail('${r.id}')">
            <div class="result-name">${esc(r.primary)}</div>
            ${r.latin !== r.primary ? `<div class="result-latin">${esc(r.latin)}</div>` : ''}
            ${common ? `<div style="color:#8b8f94;font-size:0.8rem">${esc(common)}</div>` : ''}
            <div class="result-meta">
                <span class="badge badge-cat">${r.category}</span>
                <span class="badge badge-sources">${r.n_sources} source${r.n_sources !== 1 ? 's' : ''}</span>
                ${r.has_traditional ? '<span class="badge badge-trad">Traditional</span>' : ''}
                ${r.has_evidence ? '<span class="badge badge-ev">Evidence</span>' : ''}
            </div>
            ${snippet ? `<div class="result-snippet">${esc(snippet)}</div>` : ''}
        </div>`;
    }).join('');
}

function esc(s) {
    const d = document.createElement('div');
    d.textContent = s;
    return d.innerHTML;
}

function showDetail(id) {
    modalOverlay.classList.add('active');
    modalBody.innerHTML = '<div style="color:#71767b;padding:20px">Loading...</div>';
    fetch('/api/remedy/' + encodeURIComponent(id))
        .then(r => r.json())
        .then(data => renderDetail(data));
}

function closeModal() {
    modalOverlay.classList.remove('active');
}
modalOverlay.addEventListener('click', e => {
    if (e.target === modalOverlay) closeModal();
});
document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeModal();
});

function renderDetail(d) {
    if (!d || d.error) {
        modalBody.innerHTML = '<div style="color:#ff6b6b">Remedy not found</div>';
        return;
    }
    const names = d.names || {};
    const trad = d.traditional || {};
    const ev = d.evidence || {};
    const sources = (d.provenance || {}).sources || [];
    const hasTrad = Object.keys(trad).length > 0;
    const hasEv = Object.keys(ev).length > 0;

    let html = `<h2>${esc(names.primary || d.id)}</h2>`;
    if (names.latin && names.latin !== names.primary)
        html += `<div style="color:#7ec8a0;font-style:italic;margin-bottom:4px">${esc(names.latin)}</div>`;

    const common = (names.common || []).join(', ');
    const syns = (names.synonyms || []).join(', ');
    if (common || syns) {
        html += `<div class="names-block">`;
        if (common) html += `<strong>Common:</strong> ${esc(common)}<br>`;
        if (syns) html += `<strong>Also known as:</strong> ${esc(syns)}`;
        html += `</div>`;
    }

    html += `<div class="source-list">`;
    sources.forEach(s => { html += `<span class="source-tag">${esc(s)}</span>`; });
    html += `</div>`;

    // Tabs
    const tabs = [];
    if (hasTrad) tabs.push({id: 'trad', label: 'Traditional'});
    if (hasEv) tabs.push({id: 'ev', label: 'Evidence'});
    if (!tabs.length) tabs.push({id: 'empty', label: 'Info'});

    html += `<div class="tab-bar">`;
    tabs.forEach((t, i) => {
        html += `<div class="tab ${i===0?'active':''}" onclick="switchTab('${t.id}')" data-tab="${t.id}">${t.label}</div>`;
    });
    html += `</div>`;

    // Traditional tab
    if (hasTrad) {
        html += `<div class="tab-content ${tabs[0].id==='trad'?'active':''}" id="tab-trad">`;
        html += renderBlock(trad);
        html += `</div>`;
    }

    // Evidence tab
    if (hasEv) {
        html += `<div class="tab-content ${!hasTrad?'active':''}" id="tab-ev">`;
        html += renderBlock(ev);
        html += `</div>`;
    }

    if (!hasTrad && !hasEv) {
        html += `<div class="tab-content active" id="tab-empty"><p style="color:#71767b">No detailed content available.</p></div>`;
    }

    modalBody.innerHTML = html;
}

function renderBlock(block) {
    let html = '';
    for (const [key, val] of Object.entries(block)) {
        const label = key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        if (Array.isArray(val)) {
            html += `<h4>${esc(label)}</h4>`;
            val.forEach(item => {
                if (typeof item === 'object' && item.text) {
                    const src = item.source_id || item.source || '';
                    html += `<div class="section-text">${esc(item.text.substring(0, 3000))}`;
                    if (src) html += ` <span style="color:#6bb3d9;font-size:0.75rem">[${esc(src)}]</span>`;
                    html += `</div>`;
                } else if (typeof item === 'object' && item.section) {
                    html += `<div style="color:#d9a86b;font-size:0.8rem;margin-top:6px">${esc(item.section)}</div>`;
                    if (item.text) html += `<div class="section-text">${esc(item.text.substring(0, 3000))}</div>`;
                } else if (typeof item === 'string') {
                    html += `<div class="section-text">${esc(item)}</div>`;
                }
            });
        } else if (typeof val === 'string' && val.length > 5) {
            html += `<h4>${esc(label)}</h4>`;
            html += `<div class="section-text">${esc(val.substring(0, 3000))}</div>`;
        } else if (typeof val === 'object' && !Array.isArray(val)) {
            html += `<h4>${esc(label)}</h4>`;
            html += renderBlock(val);
        }
    }
    return html;
}

function switchTab(tabId) {
    document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabId));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.toggle('active', c.id === 'tab-' + tabId));
}

// Event listeners
searchInput.addEventListener('input', () => {
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(doSearch, 250);
});
filterCategory.addEventListener('change', doSearch);
filterDual.addEventListener('change', doSearch);

// Initial load — show top remedies
doSearch();
</script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            page = HTML_PAGE.replace("REMEDY_COUNT", str(len(search_index)))
            page = page.replace("SOURCE_COUNT", "35")
            self.wfile.write(page.encode("utf-8"))

        elif path in ("/intake", "/intake/", "/intake.html"):
            intake_path = ROOT / "intake.html"
            if intake_path.exists():
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(intake_path.read_bytes())
            else:
                self.send_response(404)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(b"intake.html not found")

        elif path == "/api/search":
            params = urllib.parse.parse_qs(parsed.query)
            q = params.get("q", [""])[0]
            cat = params.get("category", [""])[0]
            dual = params.get("dual", [""])[0] == "1"
            results = search(q, category=cat, dual_only=dual)
            # Strip snippets for smaller response
            lite = []
            for r in results:
                lite.append({
                    "id": r["id"],
                    "primary": r["primary"],
                    "latin": r["latin"],
                    "common": r.get("common", [])[:3],
                    "category": r["category"],
                    "n_sources": r["n_sources"],
                    "has_traditional": r["has_traditional"],
                    "has_evidence": r["has_evidence"],
                    "snippets": {k: v[:200] for k, v in
                                 list(r.get("snippets", {}).items())[:3]},
                })
            body = json.dumps({"total": len(results), "results": lite},
                              ensure_ascii=False)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        elif path.startswith("/api/remedy/"):
            rid = urllib.parse.unquote(path[len("/api/remedy/"):])
            # Sanitize — only allow a-z, 0-9, hyphen
            rid = re.sub(r"[^a-z0-9\-]", "", rid.lower())
            data = get_remedy_detail(rid)
            if data:
                body = json.dumps(data, ensure_ascii=False)
            else:
                body = json.dumps({"error": "not found"})
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/intake":
            content_len = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(content_len)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"invalid JSON"}')
                return

            result = agent_intake_match(payload)
            body = json.dumps(result, ensure_ascii=False)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(body.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        print(f"  [{self.command}] {self.path}", flush=True)


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    server = HTTPServer(("127.0.0.1", port), Handler)
    print(f"\n  Server running at http://localhost:{port}")
    print(f"  Press Ctrl+C to stop\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.server_close()


if __name__ == "__main__":
    main()
