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
// PASSAGE INDEX — full-text search over every passage of every source
// Built by scripts/build_passage_index.py. Terms are matched INSIDE a single
// passage (~2-3 sentences), so "knee" + "motion" must appear together.
// ============================================================
const PIDX_BASE = 'data/pidx/';

const Passages = {
    meta: null,
    ready: false,
    pRem: null, pItem: null, pStart: null, pLen: null,   // per passage
    itemSec: null,                                       // per item -> section idx
    nonSymptomSec: new Set(),
    shards3: new Set(),
    autoStop: new Set(),
    shardCache: new Map(),     // shard name -> Promise<object>
    postingCache: new Map(),   // stem -> {pids: Int32Array, tfs: Uint16Array} | null
    remById: new Map(),
};

async function loadPassageIndex() {
    const statusEl = document.getElementById('loadingStatus');
    const setStatus = (msg) => { if (statusEl) statusEl.textContent = msg; };
    setStatus('Loading passage index...');
    try {
        const resp = await fetch(PIDX_BASE + '_meta.json');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const meta = await resp.json();
        const n = meta.n;
        const pRem = new Int32Array(n), pItem = new Int32Array(n);
        const pStart = new Int32Array(n), pLen = new Int32Array(n);
        const itemSec = new Int32Array(meta.items.length);
        let pid = 0;
        meta.items.forEach((it, idx) => {
            itemSec[idx] = it[1];
            const flat = it[3];
            let pos = 0;
            for (let k = 0; k < flat.length; k += 2) {
                pos += flat[k];
                pRem[pid] = it[0]; pItem[pid] = idx; pStart[pid] = pos; pLen[pid] = flat[k + 1];
                pos += flat[k + 1];
                pid++;
            }
        });
        Passages.meta = meta;
        Passages.pRem = pRem; Passages.pItem = pItem; Passages.pStart = pStart; Passages.pLen = pLen;
        Passages.itemSec = itemSec;
        Passages.nonSymptomSec = new Set(meta.non_symptom_sections.map(s => meta.sections.indexOf(s)).filter(i => i >= 0));
        Passages.shards3 = new Set(meta.shards3 || []);
        Passages.autoStop = new Set(meta.auto_stop || []);
        meta.remedies.forEach((r, i) => Passages.remById.set(r.id, i));
        Passages.ready = true;
        setStatus('');
        console.log(`Passage index: ${n} passages over ${meta.remedies.length} remedies`);
        return true;
    } catch (err) {
        setStatus('Error loading passage index: ' + err.message);
        console.error('Failed to load passage index:', err);
        return false;
    }
}

// --- Tokenizer. MUST match scripts/build_passage_index.py ---
const PIDX_STOP = new Set(`the and for with from that this are not but was has had have been were also more
very than all can its will may after before during about into over such each which
their other being gets like side does one two who when some there then only just
much most even well use used often upon many are was were his her him she they them
these those what where why how any own out off per via yet`.split(/\s+/));

function foldText(s) {
    return s.toLowerCase().replace(/æ/g, 'ae').replace(/œ/g, 'oe')
        .normalize('NFD').replace(/[̀-ͯ]/g, '');
}

function stripSuffix(w) {
    const n = w.length;
    if (w.endsWith('ies') && n > 4) return w.slice(0, -3) + 'y';
    if (w.endsWith('ied') && n > 4) return w.slice(0, -3) + 'y';
    if (w.endsWith('sses')) return w.slice(0, -2);
    for (const suf of ['ations', 'ation', 'ating', 'ated', 'ates']) {
        if (w.endsWith(suf) && n - suf.length >= 3) return w.slice(0, -suf.length) + 'at';
    }
    for (const suf of ['ing', 'ed']) {
        if (w.endsWith(suf) && n - suf.length >= 3) {
            let r = w.slice(0, -suf.length);
            if (r.length >= 4 && r[r.length - 1] === r[r.length - 2] && !'lsz'.includes(r[r.length - 1])) r = r.slice(0, -1);
            return r;
        }
    }
    if (w.endsWith('ness') && n - 4 >= 3) return w.slice(0, -4);
    if (w.endsWith('ss') || w.endsWith('us') || w.endsWith('is')) return w;
    if (w.endsWith('es') && n > 4 && ('xz'.includes(w[n - 3]) || ['ch', 'sh'].includes(w.slice(n - 4, n - 2)))) return w.slice(0, -2);
    if (w.endsWith('s') && n > 3) return w.slice(0, -1);
    return w;
}

function stemWord(w) {
    if (w.length <= 3) return w;
    let r = stripSuffix(w);
    // Mirror of scripts/build_passage_index.py stem(): dizzi/dizzy, headache/headaches, move/moving
    if (r.endsWith('i') && r.length > 3) r = r.slice(0, -1) + 'y';
    if (r.endsWith('e') && r.length >= 4) r = r.slice(0, -1);
    return r;
}

function tokenize(text) {
    const out = [];
    for (const w of (foldText(text || '').match(/[a-z]+/g) || [])) {
        if (w.length < 3 || w.length > 25 || PIDX_STOP.has(w)) continue;
        const s = stemWord(w);
        if (s.length >= 3) out.push(s);
    }
    return out;
}

// --- Postings ---
async function getPostings(stem) {
    if (Passages.postingCache.has(stem)) return Passages.postingCache.get(stem);
    const shard = Passages.shards3.has(stem.slice(0, 3)) ? stem.slice(0, 3) : stem.slice(0, 2);
    if (!Passages.shardCache.has(shard)) {
        Passages.shardCache.set(shard,
            fetch(PIDX_BASE + shard + '.json').then(r => r.ok ? r.json() : {}).catch(() => ({})));
    }
    const obj = await Passages.shardCache.get(shard);
    const enc = obj[stem];
    let decoded = null;
    if (enc) {
        const m = enc.length / 2;
        const pids = new Int32Array(m), tfs = new Uint16Array(m);
        let prev = 0;
        for (let i = 0; i < m; i++) {
            prev += enc[2 * i];
            pids[i] = prev;
            tfs[i] = enc[2 * i + 1];
        }
        decoded = { pids, tfs };
    }
    Passages.postingCache.set(stem, decoded);
    return decoded;
}

function tfAt(p, pid) {
    let lo = 0, hi = p.pids.length - 1;
    while (lo <= hi) {
        const mid = (lo + hi) >> 1;
        const v = p.pids[mid];
        if (v === pid) return p.tfs[mid];
        if (v < pid) lo = mid + 1; else hi = mid - 1;
    }
    return 0;
}

function intersectSorted(a, b) {
    const out = [];
    let i = 0, j = 0;
    while (i < a.length && j < b.length) {
        if (a[i] === b[j]) { out.push(a[i]); i++; j++; }
        else if (a[i] < b[j]) i++;
        else j++;
    }
    return out;
}

/**
 * Score every passage that contains ALL required stems (BM25), bonus stems add.
 * Returns {byRemedy: Map<remIdx, {best, top:[{pid, score}]}>, globalBest, usedRequired}
 */
async function searchGroup(requiredStems, bonusStems) {
    const N = Passages.meta.n, avgdl = Passages.meta.avg_chars;
    const k1 = 1.2, b = 0.75;
    const reqP = [], usedRequired = [];
    for (const t of new Set(requiredStems)) {
        if (Passages.autoStop.has(t)) continue;
        const p = await getPostings(t);
        if (p) { reqP.push(p); usedRequired.push(t); }
    }
    if (!reqP.length) return null;
    const bonP = [];
    for (const t of new Set(bonusStems)) {
        if (Passages.autoStop.has(t) || usedRequired.includes(t)) continue;
        const p = await getPostings(t);
        if (p) bonP.push(p);
    }

    reqP.sort((x, y) => x.pids.length - y.pids.length);
    let cand = reqP[0].pids;
    for (let i = 1; i < reqP.length; i++) cand = intersectSorted(cand, reqP[i].pids);
    if (!cand.length) return { byRemedy: new Map(), globalBest: 0, usedRequired };

    const idf = p => Math.log(1 + (N - p.pids.length + 0.5) / (p.pids.length + 0.5));
    const reqIdf = reqP.map(idf), bonIdf = bonP.map(idf);
    const byRemedy = new Map();
    let globalBest = 0;
    for (const pid of cand) {
        if (Passages.nonSymptomSec.has(Passages.itemSec[Passages.pItem[pid]])) continue;
        const norm = k1 * (1 - b + b * Passages.pLen[pid] / avgdl);
        let score = 0;
        for (let i = 0; i < reqP.length; i++) {
            const tf = tfAt(reqP[i], pid);
            score += reqIdf[i] * tf * (k1 + 1) / (tf + norm);
        }
        for (let i = 0; i < bonP.length; i++) {
            const tf = tfAt(bonP[i], pid);
            if (tf) score += 0.5 * bonIdf[i] * tf * (k1 + 1) / (tf + norm);
        }
        const ri = Passages.pRem[pid];
        let r = byRemedy.get(ri);
        if (!r) { r = { best: 0, top: [] }; byRemedy.set(ri, r); }
        r.top.push({ pid, score });
        if (score > r.best) r.best = score;
        if (score > globalBest) globalBest = score;
    }
    for (const r of byRemedy.values()) {
        r.top.sort((x, y) => y.score - x.score);
        r.top.length = Math.min(r.top.length, 3);
    }
    return { byRemedy, globalBest, usedRequired };
}

// --- Passage text resolution (reads the merged remedy file on demand) ---
const mergedCache = new Map();
function getMerged(id) {
    if (!mergedCache.has(id)) mergedCache.set(id, loadRemedyDetail(id));
    return mergedCache.get(id);
}

function sectionLabel(path) {
    return path.replace(/^relationships\._raw$/, 'relationships').replace(/\./g, ' / ')
        .replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

async function passageText(pid) {
    const it = Passages.meta.items[Passages.pItem[pid]];
    const rem = Passages.meta.remedies[it[0]];
    const secPath = Passages.meta.sections[it[1]];
    const d = await getMerged(rem.id);
    if (!d) return null;
    let node = d.traditional;
    for (const k of secPath.split('.')) node = node ? node[k] : undefined;
    let text, source = '';
    if (it[2] === -1) text = node;
    else {
        const item = Array.isArray(node) ? node[it[2]] : undefined;
        if (typeof item === 'string') text = item;
        else if (item) { text = item.text; source = item.source_id || item.source || ''; }
    }
    if (typeof text !== 'string') return null;
    const raw = text.slice(Passages.pStart[pid], Passages.pStart[pid] + Passages.pLen[pid]);
    return { pid, section: sectionLabel(secPath), source, text: raw.replace(/\s+/g, ' ').trim(), remedy: rem.id };
}

/** NIH safety text (LiverTox, LactMed, ODS...) for a remedy, if any. */
async function getSafetyNotes(id) {
    const d = await getMerged(id);
    const ev = (d && d.evidence) || {};
    // LiverTox pages come with boilerplate sections (trade names, references...) and a
    // navigation header; prefer the sections that actually talk about safety.
    const JUNK_SECTION = /^(PRODUCT INFORMATION|CHEMICAL FORMULA|CITED REFERENCES|ANNOTATED BIBLIOGRAPHY|References)/i;
    const clean = t => t.replace(/^Show details.*?Last Update:[^.]*\.\s*/i, '').replace(/\s+/g, ' ').trim();
    const pick = (key, prefer) => {
        const v = ev[key];
        if (!Array.isArray(v) || !v.length) return null;
        const items = v.map(x => typeof x === 'string' ? { text: x, section: '', source: '' } : x)
            .filter(x => x && x.text && !JUNK_SECTION.test(x.section || ''))
            .map(x => ({ ...x, text: clean(x.text) })).filter(x => x.text.length > 40);
        if (!items.length) return null;
        const best = items.find(x => prefer.test(x.section || '') || prefer.test(x.source || ''))
            || items.slice().sort((a, b) => b.text.length - a.text.length)[0];
        return { text: best.text.slice(0, 400), source: best.source || best.source_id || '' };
    };
    return {
        hepatotoxicity: pick('hepatotoxicity', /hepatotox|likelihood|overview|background/i),
        lactation: pick('lactation_safety', /summary|lactation|levels and effects/i),
        interactions: pick('documented_interactions', /interaction/i),
        adverse: pick('documented_adverse_effects', /health risks|safety|side effects|NCCIH|adverse/i),
    };
}

// ============================================================
// AI — Claude via the proxy (or a local key)
// ============================================================
const AI_MODEL = 'claude-sonnet-5-5';

const PLANNER_SYSTEM = `You are an expert homeopathic repertorization agent. You know Kent's Repertory structure, modalities, constitutional prescribing, miasms, cell salts, and the vocabulary of 19th/early-20th-century materia medica (coryza, dyspepsia, eructations, vertigo, menses, leucorrhoea, lassitude, "agg."/"amel.").

You will receive a patient's own story (what is going on and what they think it is) followed by structured intake answers. Produce a search plan for a full-text index of classical materia medica (Kent, Clarke, Hering, Boericke, Allen, Nash, King's, Ellingwood...).

HOW THE INDEX WORKS: each remedy's text is split into short passages of 2-3 sentences. A search group matches only passages that contain EVERY required term within the SAME passage. So required terms must be words that a book would plausibly write in one sentence about this symptom. Use 1-3 single words per group (not phrases); put spelling variants and synonyms in bonus_terms. Prefer the words the old books use: "worse"/"aggravation"/"agg", "better"/"amelioration"/"amel", "coryza" not "runny nose", "eructations" not "burping", "menses" not "period".

Understand CONTEXT: "knee pain worse from running" -> knee + motion/walking + worse, not "running nose". "Worse after sleep" is a Lachesis-type modality. "Desires salt" points to Natrum muriaticum. Read the patient's story as a whole, not word by word.

Return ONLY valid JSON (no markdown) with exactly this structure:
{
  "analysis": "2-3 sentences summarizing the case in homeopathic terms, including what the patient thinks is going on",
  "search_groups": [
    {
      "label": "Short human-readable description",
      "required_terms": ["1-3 single words that must co-occur in one passage"],
      "bonus_terms": ["variants/synonyms that strengthen a match"],
      "weight": 1.0 to 5.0,
      "repertory_section": "Kent chapter"
    }
  ],
  "suggested_remedies": [{"name": "Latin remedy name", "reason": "1-2 sentences tied to THIS patient"}],
  "reasoning": "Brief overall explanation",
  "urgent_warning": "Empty string, OR one plain-language sentence if anything could signal a medical emergency or something needing prompt evaluation by a doctor (chest pain, breathing trouble, stroke signs, suicidal thoughts, infant fever, bleeding in pregnancy, etc.)"
}

RULES:
1. 6-20 search groups; the most characteristic/peculiar symptoms get weight 4-5, modalities 3-5, mentals 4-5, common symptoms 1-2
2. Never put a generic word alone as the only required term ("pain", "worse", "head"); pair it with a specific one
3. suggested_remedies: up to 8, based on the totality; the user will see that these come from your knowledge, not the books
4. Fill urgent_warning whenever in doubt`;

const READER_SYSTEM = `You are an experienced classical homeopath. You will be given a patient's story and intake answers, then a list of candidate remedies, each with numbered passages quoted verbatim from classical materia medica where the patient's symptoms were found.

Your job is to READ THE PASSAGES IN CONTEXT and judge, for each remedy, whether the quoted text genuinely describes THIS patient's picture, or whether the words merely coincide (e.g. a passage about "running" as a discharge when the patient meant exercise; a modality that is the opposite of the patient's; a symptom in a different location). Weigh the patient's own account of what is going on and what they think it is. Mentals, striking/peculiar symptoms and modalities count more than common symptoms.

Also: if the patient lists medications, is pregnant/breastfeeding, or is a child, and a passage or your knowledge raises a safety concern about a remedy or the substance it is made from, say so briefly in that remedy's verdict.

Return ONLY valid JSON (no markdown):
{
  "overall": "3-5 sentences: what the books collectively say about this picture, which remedies stand out and why, and what key information is missing",
  "remedies": [
    {"n": <candidate number>, "fit": <0-10>, "verdict": "2-3 sentences citing passage numbers (e.g. P2) explaining what fits and what is coincidental", "key_passages": [<passage numbers that genuinely fit>]}
  ]
}
Include EVERY candidate exactly once. Do not add remedies that are not in the list. Be honest when nothing fits well.`;

function extractJson(text) {
    text = (text || '').trim();
    if (text.startsWith('```')) text = text.replace(/^```\w*\n?/, '').replace(/\n?```$/, '');
    const a = text.indexOf('{'), b = text.lastIndexOf('}');
    if (a === -1 || b === -1) throw new Error('No JSON object in response');
    return JSON.parse(text.slice(a, b + 1));
}

const AIState = { lastError: '' };

async function callClaude(system, userContent, maxTokens = 2500) {
    if (!Settings.hasAI) return null;
    const apiBody = JSON.stringify({
        model: AI_MODEL,
        max_tokens: maxTokens,
        system,
        messages: [{ role: 'user', content: userContent }],
    });
    const useProxy = !Settings.hasApiKey && Settings.proxyUrl;
    const url = useProxy ? Settings.proxyUrl : 'https://api.anthropic.com/v1/messages';
    const headers = { 'Content-Type': 'application/json' };
    if (!useProxy) {
        headers['x-api-key'] = Settings.apiKey;
        headers['anthropic-version'] = '2023-06-01';
        headers['anthropic-dangerous-direct-browser-access'] = 'true';
    }
    try {
        const resp = await fetch(url, { method: 'POST', headers, body: apiBody });
        if (resp.status === 429) {
            AIState.lastError = 'Daily AI limit reached';
            console.warn('Daily AI limit reached, falling back to keyword matching');
            return null;
        }
        if (!resp.ok) {
            const errText = await resp.text();
            AIState.lastError = `AI error ${resp.status}`;
            console.error('Claude API error:', resp.status, errText);
            return null;
        }
        const result = await resp.json();
        let text = '';
        for (const block of (result.content || [])) if (block.type === 'text') text += block.text;
        return extractJson(text);
    } catch (err) {
        AIState.lastError = err.message;
        console.error('Claude agent error:', err);
        return null;
    }
}

// ============================================================
// INTAKE — summary, search plan, retrieval, contextual reading
// ============================================================
function buildPatientSummary(payload) {
    const lines = [];
    const str = v => (Array.isArray(v) ? v.join(', ') : (v == null ? '' : String(v))).trim();

    // The patient's own account comes first - it is the context for everything else
    if (str(payload.story)) lines.push(`PATIENT'S OWN ACCOUNT OF WHAT IS GOING ON:\n${str(payload.story)}`);
    if (str(payload.patient_theory)) lines.push(`WHAT THE PATIENT THINKS IT IS / WHAT IS CAUSING IT:\n${str(payload.patient_theory)}`);

    const about = [];
    if (str(payload.age)) about.push(`Age: ${str(payload.age)}`);
    if (str(payload.sex)) about.push(`Sex: ${str(payload.sex)}`);
    if (str(payload.pregnancy) && !/^no\b|not applicable/i.test(str(payload.pregnancy))) about.push(`Pregnancy/breastfeeding: ${str(payload.pregnancy)}`);
    if (about.length) lines.push(`ABOUT THE PATIENT: ${about.join('; ')}`);

    const course = [];
    if (str(payload.onset)) course.push(`started ${str(payload.onset)}`);
    if (str(payload.course)) course.push(str(payload.course));
    if (str(payload.pattern)) course.push(str(payload.pattern));
    if (str(payload.severity)) course.push(`severity ${str(payload.severity)}/10`);
    if (course.length) lines.push(`COURSE: ${course.join('; ')}`);

    // Symptoms
    const sxIndices = new Set();
    for (const key of Object.keys(payload)) {
        const m = key.match(/^sx_\w+_(\d+)$/);
        if (m) sxIndices.add(parseInt(m[1]));
    }
    for (const si of [...sxIndices].sort((a, b) => a - b)) {
        const desc = str(payload[`sx_desc_${si}`]);
        const loc = str(payload[`sx_location_${si}`]);
        const sens = str(payload[`sx_sensation_${si}`]);
        const side = str(payload[`sx_side_${si}`]);
        const better = str(payload[`sx_better_${si}`]);
        const worse = str(payload[`sx_worse_${si}`]);
        if (!(desc || loc || sens)) continue;
        lines.push(`SYMPTOM ${si + 1}:`);
        if (desc) lines.push(`  Description: ${desc}`);
        if (loc) lines.push(`  Location: ${loc}`);
        if (sens) lines.push(`  Sensation: ${sens}`);
        if (side && side !== 'N/A') lines.push(`  Side: ${side}`);
        if (better) lines.push(`  Better from: ${better}`);
        if (worse) lines.push(`  Worse from: ${worse}`);
    }

    for (const [field, label] of [['thermal', 'Temperature'], ['thirst', 'Thirst'],
        ['build', 'Body Build'], ['appetite', 'Appetite']]) {
        if (str(payload[field])) lines.push(`${label}: ${str(payload[field])}`);
    }
    if (str(payload.ailments_from)) lines.push(`Ailments From / Causation: ${str(payload.ailments_from)}`);

    let checked = payload._checked || [];
    if (typeof checked === 'string') checked = [checked];
    if (checked.length) {
        const emotions = [], foodD = [], systems = [];
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

    for (const [field, label] of [['sleep_position', 'Sleep Position'],
        ['sleep_quality', 'Sleep Quality'], ['sweat_pattern', 'Perspiration'],
        ['sweat_location', 'Perspiration Location']]) {
        if (str(payload[field])) lines.push(`${label}: ${str(payload[field])}`);
    }
    for (const [field, label] of [['past_medical', 'Past Medical History'],
        ['family_history', 'Family History'], ['medications', 'Current Medications'],
        ['additional', 'Additional Notes']]) {
        if (str(payload[field])) lines.push(`${label}: ${str(payload[field])}`);
    }
    return lines.join('\n');
}

// Location / sensation vocabulary for the no-AI fallback plan
const LOCATION_TERMS = {
    "Head - Forehead": ["forehead", "frontal", "head"],
    "Head - Temples": ["temples", "temporal", "head"],
    "Head - Vertex (top)": ["vertex", "head", "top"],
    "Head - Occiput (back)": ["occiput", "occipital", "head"],
    "Head - Whole head": ["head", "headache", "cephalalgia"],
    "Eyes": ["eyes", "vision", "ocular"],
    "Ears": ["ears", "hearing", "tinnitus"],
    "Nose": ["nose", "nasal", "coryza"],
    "Throat": ["throat", "pharynx", "tonsils"],
    "Neck - Cervical": ["neck", "cervical", "nape"],
    "Back - Upper (thoracic)": ["back", "dorsal", "scapulae"],
    "Back - Lower (lumbar)": ["lumbar", "back", "lumbago", "loins"],
    "Back - Sacral": ["sacrum", "sacral", "back"],
    "Chest - General": ["chest", "thorax"],
    "Chest - Heart region": ["heart", "cardiac", "palpitation", "chest"],
    "Abdomen - Upper (epigastric)": ["epigastrium", "epigastric", "stomach", "abdomen"],
    "Abdomen - Liver region": ["liver", "hepatic", "hypochondrium"],
    "Stomach": ["stomach", "gastric", "nausea", "digestion"],
    "Rectum": ["rectum", "anus", "hemorrhoids", "haemorrhoids"],
    "Bladder": ["bladder", "urine", "urination", "cystitis"],
    "Kidneys": ["kidneys", "renal", "kidney"],
    "Knee": ["knee", "knees"],
    "Shoulder - Right": ["shoulder", "right"],
    "Shoulder - Left": ["shoulder", "left"],
    "Hip": ["hip", "hips", "hip-joint"],
    "Foot": ["foot", "feet", "soles"],
    "Skin": ["skin", "eruptions", "eruption"],
    "Joints - General": ["joints", "rheumatic", "rheumatism", "arthritic"],
    "Muscles - General": ["muscles", "muscular"],
    "Whole body / General": ["weakness", "prostration", "lassitude", "debility"],
};

const SENSATION_TERMS = {
    "Aching / Dull": ["aching", "dull", "ache"],
    "Bruised / Sore": ["bruised", "sore", "soreness", "lame"],
    "Burning": ["burning", "burn", "heat"],
    "Cramping": ["cramp", "cramping", "spasm", "spasmodic"],
    "Cutting": ["cutting", "cuts"],
    "Drawing / Pulling": ["drawing", "pulling", "tearing"],
    "Itching": ["itching", "itch", "pruritus"],
    "Numb / Tingling": ["numbness", "numb", "tingling", "formication"],
    "Pressing": ["pressing", "pressure", "pressive"],
    "Pulsating / Throbbing": ["throbbing", "pulsating", "pulsation", "beating"],
    "Sharp": ["sharp", "acute"],
    "Shooting": ["shooting", "darting", "radiating"],
    "Stitching / Stabbing": ["stitching", "stitches", "stabbing", "stitch"],
    "Stinging": ["stinging", "sting"],
    "Tearing": ["tearing", "rending"],
};

async function rarestStems(stems, count) {
    /** Pick the `count` stems with the fewest passages (most specific), ignoring unknown ones. */
    const withDf = [];
    for (const s of new Set(stems)) {
        if (Passages.autoStop.has(s)) continue;
        const p = await getPostings(s);
        if (p) withDf.push([s, p.pids.length]);
    }
    withDf.sort((a, b) => a[1] - b[1]);
    return { required: withDf.slice(0, count).map(x => x[0]), bonus: withDf.slice(count).map(x => x[0]) };
}

async function buildFallbackPlan(payload) {
    /** Search groups derived from the form alone (no AI). */
    const groups = [];
    const str = v => (Array.isArray(v) ? v.join(' ') : (v == null ? '' : String(v)));
    const tok = s => tokenize(str(s));
    const push = (label, required, bonus, weight) => {
        required = [...new Set(required)].filter(Boolean);
        if (required.length) groups.push({ label, required_terms: required, bonus_terms: bonus || [], weight });
    };

    for (const [field, w] of [['story', 2.5], ['patient_theory', 2]]) {
        const stems = tok(payload[field]);
        if (stems.length) {
            const { required, bonus } = await rarestStems(stems, 2);
            push(field === 'story' ? 'Your account' : 'What you think it is', required, bonus, w);
        }
    }

    const sxIndices = new Set();
    for (const key of Object.keys(payload)) {
        const m = key.match(/^sx_\w+_(\d+)$/);
        if (m) sxIndices.add(parseInt(m[1]));
    }
    for (const si of [...sxIndices].sort((a, b) => a - b)) {
        const loc = str(payload[`sx_location_${si}`]), sens = str(payload[`sx_sensation_${si}`]);
        const locT = (LOCATION_TERMS[loc] || tok(loc)).map(stemWord);
        const sensT = (SENSATION_TERMS[sens] || tok(sens)).map(stemWord);
        const modal = [...tok(payload[`sx_better_${si}`]), ...tok(payload[`sx_worse_${si}`])];
        if (locT.length && sensT.length) push(`${loc} + ${sens}`, [locT[0], sensT[0]], [...locT.slice(1), ...sensT.slice(1), ...modal], 4);
        else if (locT.length) push(loc, [locT[0]], [...locT.slice(1), ...modal], 2);
        else if (sensT.length) push(sens, [sensT[0]], [...sensT.slice(1), ...modal], 2);
        if (modal.length && locT.length) push(`${loc} modalities`, [locT[0], modal[0]], [...modal.slice(1), ...locT.slice(1)], 3);
        const descStems = tok(payload[`sx_desc_${si}`]);
        if (descStems.length) {
            const { required, bonus } = await rarestStems(descStems, 2);
            push(`Symptom ${si + 1} description`, required, [...bonus, ...locT, ...sensT], 3);
        }
    }

    let checked = payload._checked || [];
    if (typeof checked === 'string') checked = [checked];
    for (const item of checked) {
        const stems = tok(item.replace(/\(.*?\)/g, ''));
        if (stems.length) push(item, stems.slice(0, 2), stems.slice(2), 1.5);
    }
    if (str(payload.ailments_from)) {
        const stems = tok(payload.ailments_from);
        push(`Ailments from: ${payload.ailments_from}`, stems.slice(0, 2), stems.slice(2), 3);
    }
    for (const field of ['thermal', 'thirst', 'appetite', 'sleep_quality', 'sweat_pattern']) {
        const stems = tok(str(payload[field]).replace(/-.*$/, ''));
        if (stems.length) push(str(payload[field]), [stems[0]], stems.slice(1), 1.5);
    }
    return groups;
}

function findRemedyByName(remedyName, remedies) {
    /** Index of the remedy best matching a name from the AI, or -1. Exact name
     *  first, then whole-phrase, then every-word, so "Natrum Carbonicum" never
     *  resolves to Natrum Muriaticum just because both contain "natrum". */
    const norm = s => (s || '').toLowerCase().replace(/[^a-z ]/g, ' ').replace(/\s+/g, ' ').trim();
    const rn = norm(remedyName);
    if (!rn) return -1;
    const words = rn.split(' ').filter(w => w.length >= 3);
    let best = -1, bestRank = 0;
    for (let i = 0; i < remedies.length; i++) {
        const e = remedies[i];
        const names = [e.primary, e.latin, ...(e.common || []), ...(e.synonyms || [])]
            .map(n => norm(n).replace(/^cell salt \d* ?/, ''));
        let rank = 0;
        if (names.some(n => n === rn)) rank = 3;
        else if (names.some(n => n.includes(rn))) rank = 2;
        else if (words.length && words.every(w => names.some(n => n.includes(w)))) rank = 1;
        if (rank > bestRank || (rank && rank === bestRank && e.n_sources > remedies[best].n_sources)) {
            best = i; bestRank = rank;
        }
    }
    return best;
}

/**
 * Main intake matching.
 *   1. AI plans the search (or the form is turned into a plan without AI)
 *   2. Every group is run against the passage index; remedies are scored by
 *      how well their best passages match each group, weighted by importance
 *   3. The top candidates' actual passages are quoted and (with AI) read in
 *      context against the patient's story.
 * onStatus(msg) is called as phases progress.
 */
async function intakeMatch(payload, onStatus = () => {}) {
    if (!Passages.ready) throw new Error('Passage index not loaded');
    const remedies = Passages.meta.remedies;
    const summary = buildPatientSummary(payload);
    AIState.lastError = '';

    let plan = null;
    if (Settings.hasAI && summary.trim()) {
        onStatus('AI is reading your intake and planning the search...');
        plan = await callClaude(PLANNER_SYSTEM, `--- PATIENT INTAKE ---\n${summary}`, 2500);
        if (plan && !Array.isArray(plan.search_groups)) plan = null;
    }
    const groups = plan ? plan.search_groups : await buildFallbackPlan(payload);
    const mode = plan ? 'ai' : 'keyword';

    // --- Retrieval ---
    onStatus(`Searching ${Passages.meta.n.toLocaleString()} passages...`);
    const groupResults = await Promise.all(groups.map(async g => {
        const required = tokenize((g.required_terms || []).join(' '));
        const bonus = tokenize((g.bonus_terms || []).join(' '));
        const weight = Math.min(5, Math.max(0.5, parseFloat(g.weight) || 2));
        const res = required.length ? await searchGroup(required, bonus) : null;
        return { label: g.label || required.join(' + '), weight, res };
    }));

    const scores = new Map(), labels = new Map(), passByRem = new Map();
    for (const g of groupResults) {
        if (!g.res || !g.res.globalBest) continue;
        for (const [ri, r] of g.res.byRemedy) {
            const s = g.weight * (0.75 * r.best + 0.25 * (r.top[1] ? r.top[1].score : 0)) / g.res.globalBest;
            scores.set(ri, (scores.get(ri) || 0) + s);
            if (!labels.has(ri)) labels.set(ri, []);
            labels.get(ri).push(g.label);
            if (!passByRem.has(ri)) passByRem.set(ri, []);
            // rel = how good this passage is for its group, scaled by how much the group matters
            for (const p of r.top.slice(0, 2)) passByRem.get(ri).push({ ...p, rel: g.weight * p.score / g.res.globalBest, group: g.label });
        }
    }
    const groupsUsed = groupResults.filter(g => g.res && g.res.globalBest).length;

    // --- Candidates: top by source evidence, plus the AI's own suggestions ---
    const ranked = [...scores.entries()].sort((a, b) => b[1] - a[1]);
    const maxScore = ranked.length ? ranked[0][1] : 1;
    const candidates = ranked.slice(0, 10).map(([ri]) => ri);
    const aiReasons = new Map(), aiSuggested = new Set(), aiUnmatched = [];
    for (const entry of (plan && plan.suggested_remedies) || []) {
        const name = typeof entry === 'string' ? entry : (entry.name || '');
        const reason = typeof entry === 'object' ? (entry.reason || '') : '';
        const ri = findRemedyByName(name, remedies);
        if (ri < 0) { if (name) aiUnmatched.push({ name, reason }); continue; }
        aiSuggested.add(ri);
        if (reason && !aiReasons.has(ri)) aiReasons.set(ri, reason);
        if (!candidates.includes(ri) && candidates.length < 14) candidates.push(ri);
    }

    // --- Quote the passages behind each candidate ---
    onStatus('Pulling the matching passages from the sources...');
    const results = await Promise.all(candidates.map(async ri => {
        const e = remedies[ri];
        const seen = new Set();
        const picked = (passByRem.get(ri) || []).sort((a, b) => b.rel - a.rel)
            .filter(p => !seen.has(p.pid) && seen.add(p.pid)).slice(0, 5);
        const passages = (await Promise.all(picked.map(async p => {
            const t = await passageText(p.pid);
            return t ? { ...t, group: p.group, key: false } : null;
        }))).filter(Boolean);
        const score = scores.get(ri) || 0;
        return {
            id: e.id, primary: e.primary, latin: e.latin, category: e.category,
            cell_salt_number: e.cell_salt_number || null,
            n_sources: e.n_sources, has_traditional: e.has_traditional, has_evidence: e.has_evidence,
            score: Math.round(score * 10) / 10,
            db_score: Math.round(score * 10) / 10,
            pct: Math.round((score / maxScore) * 100),
            ai_suggested: aiSuggested.has(ri),
            ai_reason: aiReasons.get(ri) || '',
            fit: null, verdict: '',
            matched_keywords: [...new Set(labels.get(ri) || [])].slice(0, 20),
            passages,
        };
    }));

    // --- Contextual reading of the quoted passages ---
    let readingOverall = '';
    if (plan && results.some(r => r.passages.length)) {
        onStatus('AI is reading the quoted passages in context...');
        let pn = 0;
        const pmap = new Map();  // passage number -> [result idx, passage idx]
        const block = results.map((r, i) => {
            const lines = r.passages.map((p, j) => {
                pn++; pmap.set(pn, [i, j]);
                return `  P${pn} [${p.section}${p.source ? ', ' + p.source : ''}]: "${p.text.slice(0, 420)}"`;
            });
            const extra = r.ai_reason && !r.passages.length ? `  (no passages matched; suggested from AI knowledge: ${r.ai_reason})` : '';
            return `CANDIDATE ${i + 1}: ${r.primary}${r.latin && r.latin !== r.primary ? ' (' + r.latin + ')' : ''}\n${lines.join('\n')}${extra}`;
        }).join('\n\n');
        const reading = await callClaude(READER_SYSTEM,
            `--- PATIENT INTAKE ---\n${summary}\n\n--- CANDIDATES AND QUOTED PASSAGES ---\n${block}`, 3000);
        if (reading && Array.isArray(reading.remedies)) {
            readingOverall = reading.overall || '';
            for (const rr of reading.remedies) {
                const r = results[(parseInt(rr.n) || 0) - 1];
                if (!r) continue;
                const fit = parseFloat(rr.fit);
                r.fit = isNaN(fit) ? null : Math.max(0, Math.min(10, fit));
                r.verdict = rr.verdict || '';
                for (const k of (rr.key_passages || [])) {
                    const hit = pmap.get(parseInt(String(k).replace(/\D/g, '')));  // accepts 3 or "P3"
                    if (hit && hit[0] === results.indexOf(r)) r.passages[hit[1]].key = true;
                }
            }
            results.sort((a, b) => (b.fit ?? -1) - (a.fit ?? -1) || b.score - a.score);
        }
    }

    return {
        mode,
        ai_failed: Settings.hasAI && !plan,
        ai_error: AIState.lastError,
        keywords_used: groupsUsed,
        ai_analysis: plan ? (plan.analysis || '') : '',
        ai_reasoning: plan ? (plan.reasoning || '') : '',
        urgent_warning: plan ? (plan.urgent_warning || '') : '',
        ai_unmatched: aiUnmatched,
        reading_overall: readingOverall,
        results,
    };
}


// ============================================================
// EXPORTS (for use in HTML pages)
// ============================================================
window.HomeopathyDB = {
    Settings,
    loadSearchIndex,
    loadPassageIndex,
    keywordSearch,
    loadRemedyDetail,
    getSafetyNotes,
    intakeMatch,
    tokenize,
    get ready() { return dataReady; },
    get passagesReady() { return Passages.ready; },
    get remedyCount() { return Passages.ready ? Passages.meta.remedies.length : searchIndex.length; },
    get passageCount() { return Passages.ready ? Passages.meta.n : 0; },
};
