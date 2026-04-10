/**
 * Homeopathy Database Engine
 * Runs entirely client-side. Loads search index, builds inverted index,
 * handles keyword search, and calls Claude API for AI repertorization.
 */

// ============================================================
// SETTINGS (localStorage)
// ============================================================

// Cloudflare Worker proxy URL — serves your API key to all users
const AI_PROXY_URL = 'https://homeopathy-ai-proxy.daniel-rennich.workers.dev';

const Settings = {
    get apiKey() {
        return localStorage.getItem('homeopathy_api_key') || '';
    },
    set apiKey(val) {
        localStorage.setItem('homeopathy_api_key', val.trim());
    },
    get hasApiKey() {
        return this.apiKey.startsWith('sk-ant-');
    },
    get hasAI() {
        return this.hasApiKey || !!AI_PROXY_URL;
    },
    get proxyUrl() {
        return AI_PROXY_URL;
    },
};

// ============================================================
// DATA LOADING
// ============================================================
let searchIndex = [];
let inverted = {};  // token -> Set<index>
let dataReady = false;

async function loadSearchIndex() {
    const statusEl = document.getElementById('loadingStatus');
    const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg; };

    setStatus('Loading remedy database...');
    try {
        const resp = await fetch('data/search_index.json');
        searchIndex = await resp.json();
        setStatus(`Building index for ${searchIndex.length} remedies...`);

        // Build inverted index
        inverted = {};
        for (let i = 0; i < searchIndex.length; i++) {
            const e = searchIndex[i];
            const texts = [
                (e.primary || '').toLowerCase(),
                (e.latin || '').toLowerCase(),
                (e.common || []).join(' ').toLowerCase(),
                (e.synonyms || []).join(' ').toLowerCase(),
            ];
            for (const v of Object.values(e.snippets || {})) {
                texts.push(v.toLowerCase());
            }
            const combined = texts.join(' ');
            const tokens = new Set(combined.match(/[a-z]{3,}/g) || []);
            for (const token of tokens) {
                if (!inverted[token]) inverted[token] = new Set();
                inverted[token].add(i);
            }
        }
        dataReady = true;
        setStatus('');
        console.log(`Loaded ${searchIndex.length} remedies, ${Object.keys(inverted).length} tokens indexed`);
        return true;
    } catch (err) {
        setStatus('Error loading database: ' + err.message);
        console.error('Failed to load search index:', err);
        return false;
    }
}

// ============================================================
// KEYWORD SEARCH
// ============================================================
function tokenHits(word) {
    const hits = new Set();
    for (const [token, indices] of Object.entries(inverted)) {
        if (token.includes(word)) {
            for (const idx of indices) hits.add(idx);
        }
    }
    return hits;
}

function keywordSearch(query, category = '', dualOnly = false, limit = 100) {
    query = (query || '').toLowerCase().trim();
    let resultIndices;

    if (!query) {
        resultIndices = Array.from({ length: searchIndex.length }, (_, i) => i);
    } else {
        const words = query.match(/[a-z]{3,}/g);
        if (!words || !words.length) return { total: 0, results: [] };

        const sets = words.map(w => tokenHits(w));
        let combined = sets[0];
        for (let i = 1; i < sets.length; i++) {
            combined = new Set([...combined].filter(x => sets[i].has(x)));
        }
        resultIndices = [...combined].sort((a, b) => a - b);
    }

    // Filter and score
    const out = [];
    for (const idx of resultIndices) {
        const e = searchIndex[idx];
        if (category && e.category !== category) continue;
        if (dualOnly && !(e.has_traditional && e.has_evidence)) continue;

        let score = e.n_sources;
        if (query) {
            const nameLower = (e.primary + ' ' + e.latin).toLowerCase();
            if (nameLower.includes(query)) score += 100;
        }
        out.push({ ...e, _score: score });
        if (out.length >= limit * 3) break;
    }

    out.sort((a, b) => b._score - a._score);
    const results = out.slice(0, limit).map(r => ({
        id: r.id,
        primary: r.primary,
        latin: r.latin,
        common: (r.common || []).slice(0, 3),
        category: r.category,
        n_sources: r.n_sources,
        has_traditional: r.has_traditional,
        has_evidence: r.has_evidence,
        snippets: Object.fromEntries(
            Object.entries(r.snippets || {}).slice(0, 3).map(([k, v]) => [k, v.substring(0, 200)])
        ),
    }));

    return { total: out.length, results };
}

// ============================================================
// REMEDY DETAIL
// ============================================================
async function loadRemedyDetail(remedyId) {
    const safeId = remedyId.toLowerCase().replace(/[^a-z0-9\-]/g, '');
    try {
        const resp = await fetch(`data/merged/${safeId}.json`);
        if (!resp.ok) return null;
        return await resp.json();
    } catch {
        return null;
    }
}

// ============================================================
// STOP WORDS
// ============================================================
const STOP_WORDS = new Set([
    'the', 'and', 'for', 'with', 'from', 'that', 'this', 'are',
    'not', 'but', 'was', 'has', 'had', 'have', 'been', 'were',
    'also', 'more', 'very', 'than', 'all', 'can', 'its', 'will',
    'may', 'after', 'before', 'during', 'about', 'into', 'over',
    'such', 'each', 'which', 'their', 'other', 'being', 'gets',
    'like', 'side', 'does', 'one', 'two', 'who', 'when', 'some',
    'there', 'then', 'only', 'just', 'much', 'most', 'even',
    'well', 'use', 'used', 'often', 'upon', 'many',
]);

// ============================================================
// AI AGENT — Claude API (direct browser call)
// ============================================================

const REPERTORIZATION_PROMPT = `You are an expert homeopathic repertorization agent. You have deep knowledge of:
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
  "suggested_remedies": [
    {
      "name": "Latin remedy name",
      "reason": "1-2 sentence explanation of why this remedy fits this patient's specific symptom picture"
    }
  ],
  "reasoning": "Brief overall explanation of the case analysis"
}

RULES:
1. Create 5-20 search groups covering the most important symptoms
2. Put the MOST CHARACTERISTIC/PECULIAR symptoms at highest weight
3. Modalities (better/worse) are very important — weight them 3-5
4. Mental/emotional symptoms are often the most important — weight them 4-5
5. Use proper homeopathic terminology in required_terms (e.g., "aggravation" not just "worse")
6. For body locations, use both common and Latin terms
7. Group related concepts: don't search "running" alone, search ["knee", "pain", "motion", "aggravation"] together
8. Your suggested_remedies should be based on the TOTALITY, not individual symptoms. Include up to 10 remedies, each with a specific reason tied to this patient's symptoms.
9. Return ONLY valid JSON, no markdown formatting`;


function buildPatientSummary(payload) {
    const lines = [];

    // Collect symptom indices
    const sxIndices = new Set();
    for (const key of Object.keys(payload)) {
        const m = key.match(/^sx_\w+_(\d+)$/);
        if (m) sxIndices.add(parseInt(m[1]));
    }

    for (const si of [...sxIndices].sort((a, b) => a - b)) {
        const desc = payload[`sx_desc_${si}`] || '';
        const loc = payload[`sx_location_${si}`] || '';
        const sens = payload[`sx_sensation_${si}`] || '';
        const side = payload[`sx_side_${si}`] || '';
        let better = payload[`sx_better_${si}`] || [];
        let worse = payload[`sx_worse_${si}`] || [];
        if (typeof better === 'string') better = [better];
        if (typeof worse === 'string') worse = [worse];

        lines.push(`SYMPTOM ${si + 1}:`);
        if (desc) lines.push(`  Description: ${desc}`);
        if (loc) lines.push(`  Location: ${loc}`);
        if (sens) lines.push(`  Sensation: ${sens}`);
        if (side && side !== 'N/A') lines.push(`  Side: ${side}`);
        if (better.length) lines.push(`  Better from: ${better.join(', ')}`);
        if (worse.length) lines.push(`  Worse from: ${worse.join(', ')}`);
    }

    // Constitutional
    for (const [field, label] of [['thermal', 'Temperature'], ['thirst', 'Thirst'],
        ['build', 'Body Build'], ['appetite', 'Appetite']]) {
        if (payload[field]) lines.push(`${label}: ${payload[field]}`);
    }

    // Ailments from
    if (payload.ailments_from) lines.push(`Ailments From / Causation: ${payload.ailments_from}`);

    // Checked items
    let checked = payload._checked || [];
    if (typeof checked === 'string') checked = [checked];
    if (checked.length) {
        // Simple grouping
        const emotions = [], fears = [], foodD = [], foodA = [], systems = [];
        const emotionWords = ['anxiety', 'irritab', 'anger', 'sadness', 'weeping', 'restless',
            'apathy', 'fear', 'mood', 'impatien', 'jealous', 'guilt', 'despair',
            'sensitiv', 'solitude', 'company', 'excit', 'confus', 'concentrat',
            'forgetful', 'suspic', 'obstin'];
        const foodDesireSet = new Set(['Salt/salty', 'Sweets/sugar', 'Sour/acids', 'Spicy/hot',
            'Bitter', 'Fat/rich food', 'Milk', 'Cheese', 'Eggs', 'Bread', 'Meat', 'Fish',
            'Chocolate', 'Coffee', 'Tea', 'Alcohol/wine', 'Cold drinks', 'Warm drinks',
            'Ice/ice cream', 'Fruit', 'Vegetables', 'Raw food', 'Smoked food', 'Pickles',
            'Oysters', 'Onions', 'Garlic', 'Potatoes', 'Rice', 'Butter']);

        for (const c of checked) {
            const cl = c.toLowerCase();
            if (emotionWords.some(e => cl.includes(e))) emotions.push(c);
            else if (foodDesireSet.has(c)) foodD.push(c);
            else systems.push(c);
        }
        if (emotions.length) lines.push(`Emotional State: ${emotions.join(', ')}`);
        if (foodD.length) lines.push(`Food Desires: ${foodD.join(', ')}`);
        if (systems.length) lines.push(`Systems Review: ${systems.join(', ')}`);
    }

    // Sleep / perspiration
    for (const [field, label] of [['sleep_position', 'Sleep Position'],
        ['sleep_quality', 'Sleep Quality'], ['sweat_pattern', 'Perspiration'],
        ['sweat_location', 'Perspiration Location']]) {
        if (payload[field]) lines.push(`${label}: ${payload[field]}`);
    }

    // Free text
    for (const [field, label] of [['past_medical', 'Past Medical History'],
        ['family_history', 'Family History'], ['medications', 'Current Medications'],
        ['additional', 'Additional Notes']]) {
        if (payload[field] && typeof payload[field] === 'string' && payload[field].trim()) {
            lines.push(`${label}: ${payload[field]}`);
        }
    }

    return lines.join('\n');
}


async function callClaudeAgent(payload) {
    if (!Settings.hasAI) return null;

    const patientSummary = buildPatientSummary(payload);
    if (!patientSummary.trim()) return null;

    const apiBody = JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 2000,
        messages: [{
            role: 'user',
            content: `${REPERTORIZATION_PROMPT}\n\n--- PATIENT INTAKE DATA ---\n${patientSummary}`
        }]
    });

    // Decide: use local API key (direct) or proxy
    const useProxy = !Settings.hasApiKey && Settings.proxyUrl;
    const url = useProxy ? Settings.proxyUrl : 'https://api.anthropic.com/v1/messages';
    const headers = { 'Content-Type': 'application/json' };
    if (!useProxy) {
        headers['x-api-key'] = Settings.apiKey;
        headers['anthropic-version'] = '2023-06-01';
        headers['anthropic-dangerous-direct-browser-access'] = 'true';
    }

    try {
        const resp = await fetch(url, {
            method: 'POST',
            headers,
            body: apiBody,
        });

        if (resp.status === 429) {
            console.warn('Daily AI limit reached, falling back to keyword matching');
            return null;
        }
        if (!resp.ok) {
            const errText = await resp.text();
            console.error('Claude API error:', resp.status, errText);
            return null;
        }

        const result = await resp.json();
        let text = '';
        for (const block of (result.content || [])) {
            if (block.type === 'text') text += block.text;
        }
        text = text.trim();
        if (text.startsWith('```')) {
            text = text.replace(/^```\w*\n?/, '').replace(/\n?```$/, '');
        }
        return JSON.parse(text);
    } catch (err) {
        console.error('Claude agent error:', err);
        return null;
    }
}


// ============================================================
// INTAKE MATCHING — AI-powered with keyword fallback
// ============================================================

// Location to repertory terms
const LOCATION_TERMS = {
    "Head - Forehead": ["forehead", "head", "frontal"],
    "Head - Temples": ["temples", "head", "temporal"],
    "Head - Vertex (top)": ["vertex", "head", "top"],
    "Head - Occiput (back)": ["occiput", "head", "occipital"],
    "Head - Whole head": ["head", "headache", "cephalalgia"],
    "Eyes": ["eyes", "vision", "ocular"],
    "Ears": ["ears", "hearing", "tinnitus"],
    "Nose": ["nose", "nasal", "coryza"],
    "Throat": ["throat", "pharynx", "tonsils"],
    "Neck - Cervical": ["neck", "cervical"],
    "Back - Upper (thoracic)": ["back", "thoracic", "dorsal"],
    "Back - Lower (lumbar)": ["back", "lumbar", "lumbago"],
    "Back - Sacral": ["back", "sacrum", "sacral"],
    "Chest - General": ["chest"],
    "Chest - Heart region": ["heart", "chest", "cardiac", "palpitation"],
    "Abdomen - Upper (epigastric)": ["abdomen", "epigastric", "stomach"],
    "Abdomen - Liver region": ["liver", "hepatic", "abdomen"],
    "Stomach": ["stomach", "gastric", "digestion", "nausea"],
    "Rectum": ["rectum", "anus", "hemorrhoids"],
    "Bladder": ["bladder", "urinary", "cystitis"],
    "Kidneys": ["kidneys", "renal"],
    "Knee": ["knee"],
    "Shoulder - Right": ["shoulder", "right"],
    "Shoulder - Left": ["shoulder", "left"],
    "Hip": ["hip"],
    "Foot": ["foot", "feet"],
    "Skin": ["skin", "eruptions", "dermatitis"],
    "Joints - General": ["joints", "arthritis", "rheumatic"],
    "Muscles - General": ["muscles", "muscular"],
    "Whole body / General": ["general", "weakness", "fatigue"],
};

const SENSATION_TERMS = {
    "Aching / Dull": ["aching", "dull", "pain"],
    "Bruised / Sore": ["bruised", "sore", "lame"],
    "Burning": ["burning", "burn", "heat"],
    "Cramping": ["cramping", "cramp", "spasm"],
    "Cutting": ["cutting"],
    "Drawing / Pulling": ["drawing", "pulling", "tearing"],
    "Itching": ["itching", "itch", "pruritus"],
    "Numb / Tingling": ["numbness", "tingling", "numb"],
    "Pressing": ["pressing", "pressure"],
    "Pulsating / Throbbing": ["pulsating", "throbbing"],
    "Sharp": ["sharp"],
    "Shooting": ["shooting", "radiating"],
    "Stitching / Stabbing": ["stitching", "stabbing", "stitch"],
    "Stinging": ["stinging", "sting"],
    "Tearing": ["tearing"],
};


function keywordFallbackMatch(payload) {
    /** Fallback matching when AI agent is unavailable. Context-aware keyword search. */
    const scores = {};
    const matchedKw = {};
    const searchPhrases = [];

    function award(idx, points, label) {
        scores[idx] = (scores[idx] || 0) + points;
        if (!matchedKw[idx]) matchedKw[idx] = [];
        if (!matchedKw[idx].includes(label)) matchedKw[idx].push(label);
    }

    // Collect symptom indices
    const sxIndices = new Set();
    for (const key of Object.keys(payload)) {
        const m = key.match(/^sx_\w+_(\d+)$/);
        if (m) sxIndices.add(parseInt(m[1]));
    }

    for (const si of [...sxIndices].sort()) {
        const loc = payload[`sx_location_${si}`] || '';
        const sens = payload[`sx_sensation_${si}`] || '';
        const desc = payload[`sx_desc_${si}`] || '';

        const locTerms = LOCATION_TERMS[loc] || (loc ? (loc.toLowerCase().match(/[a-z]{3,}/g) || []).filter(w => !STOP_WORDS.has(w)) : []);
        const sensTerms = SENSATION_TERMS[sens] || (sens ? (sens.toLowerCase().match(/[a-z]{3,}/g) || []).filter(w => !STOP_WORDS.has(w)) : []);

        // Context-aware: location + sensation together
        if (locTerms.length && sensTerms.length) {
            let locHits = new Set();
            for (const lt of locTerms) for (const idx of tokenHits(lt)) locHits.add(idx);
            let sensHits = new Set();
            for (const st of sensTerms) for (const idx of tokenHits(st)) sensHits.add(idx);
            const combo = new Set([...locHits].filter(x => sensHits.has(x)));
            const label = `${loc} + ${sens}`;
            searchPhrases.push(label);
            for (const idx of combo) award(idx, 5.0, label);
            for (const idx of locHits) if (!combo.has(idx)) award(idx, 1.0, loc);
            for (const idx of sensHits) if (!combo.has(idx)) award(idx, 1.0, sens);
        } else if (locTerms.length) {
            searchPhrases.push(loc);
            for (const lt of locTerms) for (const idx of tokenHits(lt)) award(idx, 2.0, loc);
        } else if (sensTerms.length) {
            searchPhrases.push(sens);
            for (const st of sensTerms) for (const idx of tokenHits(st)) award(idx, 2.0, sens);
        }

        // Description — 2-word phrase windows
        if (desc) {
            const words = (desc.toLowerCase().match(/[a-z]{3,}/g) || []).filter(w => !STOP_WORDS.has(w));
            for (let w = 2; w <= Math.min(3, words.length); w++) {
                for (let i = 0; i <= words.length - w; i++) {
                    const phraseWords = words.slice(i, i + w);
                    let hits = tokenHits(phraseWords[0]);
                    for (let j = 1; j < phraseWords.length; j++) {
                        const next = tokenHits(phraseWords[j]);
                        hits = new Set([...hits].filter(x => next.has(x)));
                    }
                    const label = phraseWords.join(' ');
                    if (hits.size) {
                        searchPhrases.push(label);
                        for (const idx of hits) award(idx, 3.0, label);
                    }
                }
            }
        }
    }

    // Checked items
    let checked = payload._checked || [];
    if (typeof checked === 'string') checked = [checked];
    for (const item of checked) {
        const words = (item.toLowerCase().match(/[a-z]{3,}/g) || []).filter(w => !STOP_WORDS.has(w));
        if (!words.length) continue;
        searchPhrases.push(item);
        if (words.length >= 2) {
            let hits = tokenHits(words[0]);
            for (const w of words.slice(1)) hits = new Set([...hits].filter(x => tokenHits(w).has(x)));
            for (const idx of hits) award(idx, 2.0, item);
        } else {
            for (const idx of tokenHits(words[0])) award(idx, 1.5, item);
        }
    }

    // Ailments from
    if (payload.ailments_from) {
        const words = (payload.ailments_from.toLowerCase().match(/[a-z]{3,}/g) || []).filter(w => !STOP_WORDS.has(w));
        const label = `Ailments from: ${payload.ailments_from}`;
        searchPhrases.push(label);
        if (words.length >= 2) {
            let hits = tokenHits(words[0]);
            for (const w of words.slice(1)) hits = new Set([...hits].filter(x => tokenHits(w).has(x)));
            for (const idx of hits) award(idx, 4.0, label);
        } else if (words.length) {
            for (const idx of tokenHits(words[0])) award(idx, 3.0, label);
        }
    }

    // Constitutional fields
    for (const field of ['thermal', 'thirst', 'appetite']) {
        if (payload[field]) {
            const words = (payload[field].toLowerCase().match(/[a-z]{3,}/g) || []).filter(w => !STOP_WORDS.has(w));
            searchPhrases.push(payload[field]);
            for (const w of words) for (const idx of tokenHits(w)) award(idx, 1.5, payload[field]);
        }
    }

    if (!Object.keys(scores).length) return { keywords_used: 0, results: [] };

    const ranked = Object.entries(scores).sort((a, b) => b[1] - a[1]).slice(0, 10);
    const maxScore = ranked.length ? ranked[0][1] : 1;
    const results = ranked.map(([idx, score]) => {
        const e = searchIndex[parseInt(idx)];
        return {
            id: e.id, primary: e.primary, latin: e.latin, category: e.category,
            n_sources: e.n_sources, has_traditional: e.has_traditional,
            has_evidence: e.has_evidence, score: Math.round(score * 10) / 10,
            pct: Math.round((score / maxScore) * 100),
            ai_reason: '',
            matched_keywords: (matchedKw[idx] || []).slice(0, 20),
        };
    });

    return { keywords_used: new Set(searchPhrases).size, results };
}


async function intakeMatch(payload) {
    /** Main intake matching — tries AI agent first, falls back to keywords. */

    // Try AI agent
    const agentResult = await callClaudeAgent(payload);

    if (agentResult && agentResult.search_groups) {
        // Execute the AI's search plan against our database
        const scores = {};
        const matchedKw = {};
        const searchPhrases = [];

        function award(idx, points, label) {
            scores[idx] = (scores[idx] || 0) + points;
            if (!matchedKw[idx]) matchedKw[idx] = [];
            if (!matchedKw[idx].includes(label)) matchedKw[idx].push(label);
        }

        for (const group of agentResult.search_groups) {
            const label = group.label || '';
            const required = (group.required_terms || []).map(t => t.toLowerCase()).filter(t => t.length >= 3);
            const bonus = (group.bonus_terms || []).map(t => t.toLowerCase()).filter(t => t.length >= 3);
            const weight = parseFloat(group.weight) || 2.0;

            if (!required.length) continue;
            searchPhrases.push(label);

            // AND logic — must match all required terms
            const termHits = required.map(t => tokenHits(t));
            let combined = termHits[0];
            for (let i = 1; i < termHits.length; i++) {
                combined = new Set([...combined].filter(x => termHits[i].has(x)));
            }
            for (const idx of combined) award(idx, weight, label);

            // Bonus terms boost already-matched remedies
            for (const bt of bonus) {
                const btHits = tokenHits(bt);
                for (const idx of btHits) {
                    if (scores[idx]) award(idx, weight * 0.3, `+ ${bt}`);
                }
            }
        }

        // Boost Claude's suggested remedies and capture per-remedy reasons
        const aiReasons = {};  // remedy index -> reason string
        const suggested = agentResult.suggested_remedies || [];
        for (const entry of suggested) {
            // Handle both old format (string) and new format ({name, reason})
            const remedyName = typeof entry === 'string' ? entry : (entry.name || '');
            const reason = typeof entry === 'object' ? (entry.reason || '') : '';
            const rnLower = remedyName.toLowerCase();
            for (let i = 0; i < searchIndex.length; i++) {
                const e = searchIndex[i];
                const nameText = [e.primary, e.latin, ...(e.common || []), ...(e.synonyms || [])]
                    .join(' ').toLowerCase();
                if (nameText.includes(rnLower) ||
                    rnLower.split(/\s+/).filter(w => w.length >= 4).some(w => nameText.includes(w))) {
                    award(i, 8.0, `AI suggested: ${remedyName}`);
                    if (reason) aiReasons[i] = reason;
                    break;
                }
            }
        }

        if (Object.keys(scores).length) {
            const ranked = Object.entries(scores).sort((a, b) => b[1] - a[1]).slice(0, 10);
            const maxScore = ranked.length ? ranked[0][1] : 1;
            const results = ranked.map(([idx, score]) => {
                const e = searchIndex[parseInt(idx)];
                return {
                    id: e.id, primary: e.primary, latin: e.latin, category: e.category,
                    n_sources: e.n_sources, has_traditional: e.has_traditional,
                    has_evidence: e.has_evidence,
                    score: Math.round(score * 10) / 10,
                    pct: Math.round((score / maxScore) * 100),
                    ai_reason: aiReasons[parseInt(idx)] || '',
                    matched_keywords: (matchedKw[idx] || []).slice(0, 20),
                };
            });

            return {
                keywords_used: new Set(searchPhrases).size,
                ai_analysis: agentResult.analysis || '',
                ai_reasoning: agentResult.reasoning || '',
                results,
            };
        }
    }

    // Fallback to keyword matching
    return keywordFallbackMatch(payload);
}


// ============================================================
// EXPORTS (for use in HTML pages)
// ============================================================
window.HomeopathyDB = {
    Settings,
    loadSearchIndex,
    keywordSearch,
    loadRemedyDetail,
    intakeMatch,
    get ready() { return dataReady; },
    get remedyCount() { return searchIndex.length; },
};
