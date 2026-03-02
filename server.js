const http = require("http");
const { Pool } = require("pg");

// Use global fetch if available (Node 18+), otherwise undici
let fetchFn = global.fetch;
if (!fetchFn) {
  try {
    fetchFn = require("undici").fetch;
  } catch {
    fetchFn = null;
  }
}

// --------------------
// DB
// --------------------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : undefined
});

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      event_type TEXT NOT NULL,
      details JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_config (
      key TEXT PRIMARY KEY,
      value JSONB NOT NULL DEFAULT '{}'::jsonb,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS owner_alerts (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      severity TEXT NOT NULL DEFAULT 'info',
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      meta JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);

  // ✅ NEW: Designated lanes table (Lovable-controlled)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS designated_lanes (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      is_active BOOLEAN NOT NULL DEFAULT TRUE,

      pickup_city TEXT,
      pickup_state TEXT,
      delivery_city TEXT,
      delivery_state TEXT,

      // Optional: exact string lane key if you prefer "City, ST -> City, ST"
      lane_key TEXT,

      notes TEXT,
      created_by TEXT
    );
  `);

  // Helpful index for quick matching
  await pool.query(`
    CREATE INDEX IF NOT EXISTS designated_lanes_active_idx
    ON designated_lanes (is_active);
  `);
}

async function logEvent(eventType, details = {}) {
  try {
    await pool.query(
      `INSERT INTO audit_log (event_type, details) VALUES ($1, $2::jsonb)`,
      [eventType, JSON.stringify(details)]
    );
  } catch (e) {
    console.error("Audit log insert failed:", e?.message || e);
  }
}

async function setConfig(key, value) {
  await pool.query(
    `
    INSERT INTO app_config (key, value, updated_at)
    VALUES ($1, $2::jsonb, NOW())
    ON CONFLICT (key) DO UPDATE
      SET value = EXCLUDED.value, updated_at = NOW()
    `,
    [key, JSON.stringify(value)]
  );
}

async function getConfig(key) {
  const r = await pool.query(`SELECT value FROM app_config WHERE key = $1`, [key]);
  return r.rows?.[0]?.value ?? null;
}

// --------------------
// Owner Alerts
// --------------------
async function createAlert({ severity = "info", title, message, meta = {} }) {
  await pool.query(
    `INSERT INTO owner_alerts (severity, title, message, meta)
     VALUES ($1, $2, $3, $4::jsonb)`,
    [String(severity), String(title), String(message), JSON.stringify(meta || {})]
  );
}

async function listAlerts(limit = 25) {
  const lim = Math.max(1, Math.min(100, Number(limit) || 25));
  const r = await pool.query(
    `SELECT id, ts, severity, title, message, meta
     FROM owner_alerts
     ORDER BY id DESC
     LIMIT $1`,
    [lim]
  );
  return r.rows || [];
}

// --------------------
// ✅ Designated Lanes - DB helpers
// --------------------
function cleanStr(x) {
  if (x === null || x === undefined) return null;
  const s = String(x).trim();
  return s.length ? s : null;
}
function normCity(x) {
  const s = cleanStr(x);
  return s ? s.toLowerCase() : null;
}
function normState(x) {
  const s = cleanStr(x);
  return s ? s.toUpperCase() : null;
}
function makeLaneKey(pickupCity, pickupState, deliveryCity, deliveryState) {
  const pc = normCity(pickupCity);
  const ps = normState(pickupState);
  const dc = normCity(deliveryCity);
  const ds = normState(deliveryState);
  if (!pc || !ps || !dc || !ds) return null;
  return `${pc},${ps}->${dc},${ds}`;
}

async function listDesignatedLanes(limit = 200) {
  const lim = Math.max(1, Math.min(500, Number(limit) || 200));
  const r = await pool.query(
    `SELECT id, created_at, updated_at, is_active,
            pickup_city, pickup_state, delivery_city, delivery_state,
            lane_key, notes, created_by
     FROM designated_lanes
     WHERE is_active = TRUE
     ORDER BY id DESC
     LIMIT $1`,
    [lim]
  );
  return r.rows || [];
}

async function addDesignatedLane({ pickup_city, pickup_state, delivery_city, delivery_state, lane_key, notes, created_by }) {
  const pc = cleanStr(pickup_city);
  const ps = normState(pickup_state);
  const dc = cleanStr(delivery_city);
  const ds = normState(delivery_state);

  let lk = cleanStr(lane_key);
  if (!lk) lk = makeLaneKey(pc, ps, dc, ds);

  const r = await pool.query(
    `INSERT INTO designated_lanes
      (pickup_city, pickup_state, delivery_city, delivery_state, lane_key, notes, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     RETURNING id, lane_key`,
    [pc, ps, dc, ds, lk, cleanStr(notes), cleanStr(created_by)]
  );
  return r.rows?.[0] || null;
}

async function removeDesignatedLaneById(id) {
  await pool.query(
    `UPDATE designated_lanes
     SET is_active = FALSE, updated_at = NOW()
     WHERE id = $1`,
    [Number(id)]
  );
}

async function removeDesignatedLaneByKey(laneKey) {
  await pool.query(
    `UPDATE designated_lanes
     SET is_active = FALSE, updated_at = NOW()
     WHERE lane_key = $1 AND is_active = TRUE`,
    [String(laneKey)]
  );
}

async function replaceDesignatedLanesBulk(lanes = [], created_by = "lovable_bulk") {
  // Soft-disable all current active lanes
  await pool.query(`UPDATE designated_lanes SET is_active = FALSE, updated_at = NOW() WHERE is_active = TRUE`);

  // Insert new set
  for (const lane of lanes) {
    await addDesignatedLane({
      pickup_city: lane.pickup_city,
      pickup_state: lane.pickup_state,
      delivery_city: lane.delivery_city,
      delivery_state: lane.delivery_state,
      lane_key: lane.lane_key,
      notes: lane.notes,
      created_by
    });
  }
}

// --------------------
// HTTP helpers
// --------------------
function unauthorized(res) {
  res.writeHead(401, { "Content-Type": "text/plain" });
  res.end("Unauthorized");
}

function json(res, status, obj) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", chunk => (data += chunk));
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

function safeJsonParse(text) {
  try {
    return { ok: true, value: JSON.parse(text || "{}") };
  } catch {
    return { ok: false, value: null };
  }
}

// --------------------
// OpenAI helper (JSON only) - used for /draft/carrier-reply
// --------------------
async function openaiChatJSON({ system, user, maxTokens = 900 }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) return { error: "OPENAI_API_KEY not set" };
  if (!fetchFn) return { error: "fetch not available (use Node 18+ or install undici)" };

  const payload = {
    model: "gpt-4.1-mini",
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ],
    temperature: 0,
    max_tokens: maxTokens,
    response_format: { type: "json_object" }
  };

  const resp = await fetchFn("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!resp.ok) {
    const errText = await resp.text();
    return { error: `OpenAI error: ${resp.status} ${errText}` };
  }

  const data = await resp.json();
  const content = data?.choices?.[0]?.message?.content?.trim() || "{}";

  const parsed = safeJsonParse(content);
  if (!parsed.ok) return { error: "Model returned non-JSON", raw: content };
  return parsed.value;
}

// --------------------
// Helpers
// --------------------
function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

/**
 * Accepts numbers and strings like "$600", "41,500", "600.00"
 */
function numOrNull(x) {
  if (x === null || x === undefined) return null;
  if (typeof x === "number") return Number.isFinite(x) ? x : null;

  const s = String(x).trim().replace(/[$,]/g, "");
  if (!s) return null;

  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function roundTo5(n) {
  return Math.round(n / 5) * 5;
}

function midpointFromLowHigh(low, high) {
  const l = numOrNull(low);
  const h = numOrNull(high);
  if (l == null || h == null) return null;
  if (l <= 0 || h <= 0) return null;
  return Math.round((l + h) / 2);
}

function parseRateFromText(text) {
  const t = String(text || "");
  const m1 = t.match(/\$\s*([0-9]{3,5}(?:,[0-9]{3})?)(?:\.[0-9]{1,2})?/);
  if (m1) return numOrNull(m1[1]);
  const m2 = t.match(/\b([0-9]{3,5})\b/);
  if (m2) return numOrNull(m2[1]);
  return null;
}

function looksLikeRateRequest(emailText) {
  const t = String(emailText || "").toLowerCase();
  return /\brate\??\b/.test(t) || /\bwhat.*rate\b/.test(t) || /\bprice\??\b/.test(t);
}

function normalizeLoadContext(ctx = {}) {
  const safe = {
    pickup_fcfs_or_appt: ctx.pickup_fcfs_or_appt ?? null,
    pickup_time_window_military: ctx.pickup_time_window_military ?? null,
    pickup_city_state: ctx.pickup_city_state ?? null,
    delivery_fcfs_or_appt: ctx.delivery_fcfs_or_appt ?? null,
    delivery_time_window_military: ctx.delivery_time_window_military ?? null,
    delivery_city_state: ctx.delivery_city_state ?? null,
    weight_lbs: ctx.weight_lbs ?? null,
    commodity_desc: ctx.commodity_desc ?? null,

    posted_rate: ctx.posted_rate ?? null,
    midpoint_rate: ctx.midpoint_rate ?? null,
    rateview_low: ctx.rateview_low ?? null,
    rateview_high: ctx.rateview_high ?? null,
    lane_guess: ctx.lane_guess ?? null
  };

  for (const k of Object.keys(safe)) {
    if (typeof safe[k] === "string") safe[k] = safe[k].trim();
  }
  return safe;
}

// Message templates (your exact phrasing)
function msgAround(rate) {
  return `We are looking to be around $${rate}.`;
}
function msgCouldDo(rate) {
  return `We could do $${rate}.`;
}
function msgBestIs(rate) {
  return `My best is $${rate}.`;
}

// Your 685-style offer logic
function computeNextOffer({ postedRate, carrierAsk, mid }) {
  // posted + 35% of gap, bump clamped 60..120, rounded to 5, never above midpoint
  const gap = carrierAsk - postedRate;
  let bump = gap * 0.35;
  bump = Math.max(60, Math.min(120, bump));
  let offer = postedRate + bump;
  offer = roundTo5(offer);
  offer = Math.min(offer, mid);
  return Math.round(offer);
}

// --------------------
// ✅ Designated Lane match helper
// --------------------
function splitCityState(cityStateStr) {
  const s = cleanStr(cityStateStr);
  if (!s) return { city: null, state: null };
  // Accept formats: "Seymour IN" | "Seymour, IN" | "Seymour,IN"
  const m = s.match(/^(.+?)[,\s]+([A-Za-z]{2})$/);
  if (!m) return { city: s, state: null };
  return { city: m[1].trim(), state: m[2].toUpperCase() };
}

async function isDesignatedLane(loadContext) {
  // Try to build lane from load_context pickup/delivery city/state strings
  const pick = splitCityState(loadContext?.pickup_city_state);
  const del = splitCityState(loadContext?.delivery_city_state);

  const laneKey = makeLaneKey(pick.city, pick.state, del.city, del.state);
  if (!laneKey) return false;

  const r = await pool.query(
    `SELECT id FROM designated_lanes
     WHERE is_active = TRUE AND lane_key = $1
     LIMIT 1`,
    [laneKey]
  );
  return (r.rows?.length || 0) > 0;
}

// --------------------
// Server
// --------------------
let tablesReady = false;

const server = http.createServer(async (req, res) => {
  // AUTH (locked server)
  const token = req.headers["authorization"];
  const expected = process.env.AUTH_TOKEN;

  if (!expected) return json(res, 500, { error: "Server misconfigured: missing AUTH_TOKEN" });
  if (!token || token !== `Bearer ${expected}`) return unauthorized(res);

  if (!tablesReady) {
    try {
      await ensureTables();
      tablesReady = true;
      await logEvent("server_start", { ok: true });
    } catch (e) {
      return json(res, 500, { error: "DB init failed", details: e?.message || String(e) });
    }
  }

  const urlObj = new URL(req.url, "https://dummy.local");
  const pathname = urlObj.pathname;

  // Health
  if (pathname === "/health" && req.method === "GET") {
    await logEvent("health_check", {});
    return json(res, 200, { ok: true });
  }

  // --------------------
  // ✅ Designated lane endpoints (Lovable control panel)
  // --------------------
  if (pathname === "/lanes/designated" && req.method === "GET") {
    const limit = urlObj.searchParams.get("limit") || "200";
    const lanes = await listDesignatedLanes(limit);
    await logEvent("designated_lanes_listed", { count: lanes.length });
    return json(res, 200, { ok: true, lanes });
  }

  if (pathname === "/lanes/designated" && req.method === "POST") {
    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const b = parsed.value || {};
    const pickup_city = b.pickup_city;
    const pickup_state = b.pickup_state;
    const delivery_city = b.delivery_city;
    const delivery_state = b.delivery_state;
    const notes = b.notes || "";
    const created_by = b.created_by || "lovable";

    // Either explicit lane_key OR all city/state required
    const lane_key = b.lane_key || null;

    const inserted = await addDesignatedLane({
      pickup_city,
      pickup_state,
      delivery_city,
      delivery_state,
      lane_key,
      notes,
      created_by
    });

    await logEvent("designated_lane_added", { id: inserted?.id, lane_key: inserted?.lane_key });

    return json(res, 200, { ok: true, added: inserted });
  }

  if (pathname === "/lanes/designated" && req.method === "DELETE") {
    const id = urlObj.searchParams.get("id");
    const lane_key = urlObj.searchParams.get("lane_key");

    if (id) {
      await removeDesignatedLaneById(id);
      await logEvent("designated_lane_removed", { id });
      return json(res, 200, { ok: true, removed: { id: Number(id) } });
    }

    if (lane_key) {
      await removeDesignatedLaneByKey(lane_key);
      await logEvent("designated_lane_removed", { lane_key });
      return json(res, 200, { ok: true, removed: { lane_key } });
    }

    return json(res, 400, { error: "Provide ?id= or ?lane_key=" });
  }

  if (pathname === "/lanes/designated/bulk" && req.method === "POST") {
    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const b = parsed.value || {};
    const lanes = Array.isArray(b.lanes) ? b.lanes : [];
    const created_by = b.created_by || "lovable_bulk";

    await replaceDesignatedLanesBulk(lanes, created_by);
    await logEvent("designated_lanes_bulk_replace", { count: lanes.length });

    return json(res, 200, { ok: true, replaced_count: lanes.length });
  }

  // Rules config
  if (pathname === "/config/rules" && req.method === "POST") {
    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) {
      await logEvent("config_rules_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }
    await setConfig("rules", parsed.value);
    await logEvent("config_rules_saved", { keys: Object.keys(parsed.value || {}) });
    return json(res, 200, { ok: true });
  }

  if (pathname === "/config/rules" && req.method === "GET") {
    const rules = await getConfig("rules");
    await logEvent("config_rules_read", {});
    return json(res, 200, { rules });
  }

  // Alerts
  if (pathname === "/alerts" && req.method === "POST") {
    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) {
      await logEvent("alerts_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    const body = parsed.value;
    const severity = (body.severity || "info").toString();
    const title = (body.title || "").toString().slice(0, 140);
    const message = (body.message || "").toString().slice(0, 2000);
    const meta = body.meta && typeof body.meta === "object" ? body.meta : {};

    if (!title || !message) return json(res, 400, { error: "Missing title or message" });

    await createAlert({ severity, title, message, meta });
    await logEvent("alert_created", { severity, title });

    return json(res, 200, { ok: true });
  }

  if (pathname === "/alerts" && req.method === "GET") {
    const limit = urlObj.searchParams.get("limit") || "25";
    const alerts = await listAlerts(limit);
    await logEvent("alerts_listed", { count: alerts.length });
    return json(res, 200, { ok: true, alerts });
  }

  // Draft carrier reply
  if (pathname === "/draft/carrier-reply" && req.method === "POST") {
    const rules = (await getConfig("rules")) || {};
    const safetyMode = rules?.safety?.mode || "draft_only";

    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) {
      await logEvent("draft_reply_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    const body = parsed.value;
    const emailText = (body.email_text || "").toString().slice(0, 12000);
    if (!emailText) return json(res, 400, { error: "Missing email_text" });

    const loadContext = normalizeLoadContext(body.load_context || {});

    // ✅ HARD GUARDRAIL: designated lanes are owner-only
    const designated = await isDesignatedLane(loadContext);
    if (designated) {
      try {
        await createAlert({
          severity: "high",
          title: "Designated lane — AI blocked",
          message: `Carrier email received on designated lane. AI will not respond.`,
          meta: { lane_guess: loadContext?.lane_guess || null, pickup: loadContext?.pickup_city_state, delivery: loadContext?.delivery_city_state }
        });
      } catch {}

      await logEvent("draft_carrier_reply_blocked_designated", {
        pickup: loadContext?.pickup_city_state,
        delivery: loadContext?.delivery_city_state
      });

      return json(res, 200, {
        ok: true,
        result: {
          blocked: true,
          reason: "designated_lane",
          draft_reply_text: "",
          notes_for_owner: "Designated lane — handle manually."
        }
      });
    }

    const system = `
You are DKT Logistics' carrier email assistant.
SAFETY MODE: ${safetyMode}. Draft only.

Hard requirements:
- Format EXACTLY:
  1) Picks up {pickup_fcfs_or_appt} {pickup_time_window_military} in {pickup_city_state}
  2) Delivers to {delivery_city_state} {delivery_fcfs_or_appt} {delivery_time_window_military}
  3) Truckload of {commodity_desc} weighing {weight_lbs}lbs
- Ask for MC ONLY if not found.
- If load_context has details, DO NOT ask to confirm them.
- No fluff. No "thanks".

Return ONLY JSON with:
lane_guess, mc_found, mc_value, needs_mc_request, draft_reply_text, negotiation_next_line, notes_for_owner
`;

    const user = `
RULES:
${JSON.stringify(rules)}

EMAIL:
${emailText}

LOAD_CONTEXT (trust if present):
${JSON.stringify(loadContext)}
`;

    const result = await openaiChatJSON({ system, user, maxTokens: 900 });

    await logEvent("draft_carrier_reply", {
      lane_guess: result?.lane_guess,
      mc_found: result?.mc_found
    });

    try {
      if (result?.needs_mc_request) {
        await createAlert({
          severity: "info",
          title: "MC needed",
          message: `Carrier asked about lane ${result?.lane_guess || "(unknown)"} but did not provide MC.`,
          meta: { lane: result?.lane_guess || null }
        });
      }
    } catch {}

    return json(res, 200, { ok: true, result });
  }

  // Draft negotiation (unchanged logic, but now blocks designated lanes)
  if (pathname === "/draft/negotiate" && req.method === "POST") {
    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) {
      await logEvent("draft_negotiate_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    const body = parsed.value;
    const emailText = (body.email_text || "").toString().slice(0, 12000);

    const loadContext = normalizeLoadContext(body.load_context || {});

    // Fallback: accept fields at top level too
    if (loadContext.posted_rate == null && body.posted_rate != null) loadContext.posted_rate = body.posted_rate;
    if (loadContext.midpoint_rate == null && body.midpoint_rate != null) loadContext.midpoint_rate = body.midpoint_rate;
    if (loadContext.rateview_low == null && body.rateview_low != null) loadContext.rateview_low = body.rateview_low;
    if (loadContext.rateview_high == null && body.rateview_high != null) loadContext.rateview_high = body.rateview_high;

    // ✅ HARD GUARDRAIL: block designated lanes
    const designated = await isDesignatedLane(loadContext);
    if (designated) {
      try {
        await createAlert({
          severity: "high",
          title: "Designated lane — negotiation blocked",
          message: `AI blocked negotiation attempt on designated lane.`,
          meta: { pickup: loadContext?.pickup_city_state, delivery: loadContext?.delivery_city_state }
        });
      } catch {}

      await logEvent("draft_negotiate_blocked_designated", {
        pickup: loadContext?.pickup_city_state,
        delivery: loadContext?.delivery_city_state
      });

      return json(res, 200, {
        ok: true,
        result: {
          decision: "blocked_designated_lane",
          draft_reply_text: "",
          reasoning: "Designated lane: owner-controlled; AI must not negotiate."
        }
      });
    }

    const lane = loadContext.lane_guess || null;

    const postedRate = numOrNull(loadContext.posted_rate);
    const low = numOrNull(loadContext.rateview_low);
    const high = numOrNull(loadContext.rateview_high);
    const mid = numOrNull(loadContext.midpoint_rate) ?? midpointFromLowHigh(low, high);

    // parse carrier ask
    let carrierAsk = numOrNull(body.carrier_ask_rate);
    if (carrierAsk == null) carrierAsk = parseRateFromText(emailText);

    // stage control
    const stage = String(body.stage || "").toLowerCase();
    const noResponseAfterMidpoint = body.no_response_after_midpoint === true;

    if (postedRate == null || postedRate <= 0) {
      await logEvent("draft_negotiate_error", { reason: "missing_posted_rate" });
      return json(res, 400, { error: "Missing/invalid load_context.posted_rate" });
    }
    if (mid == null || mid <= 0) {
      await logEvent("draft_negotiate_error", { reason: "missing_midpoint", low, high, midpoint_rate: loadContext.midpoint_rate });
      return json(res, 400, {
        error: "Missing/invalid midpoint. Provide midpoint_rate OR rateview_low & rateview_high.",
        debug: { received_low: loadContext.rateview_low, received_high: loadContext.rateview_high, received_midpoint_rate: loadContext.midpoint_rate }
      });
    }

    // Owner alert if carrier ask above high
    if (high != null && high > 0 && carrierAsk != null && carrierAsk > high) {
      const owner_alert = {
        severity: "high",
        title: "Carrier ask above high",
        message: `Carrier asked $${carrierAsk} (above high $${high}) for ${lane || "lane"}.`,
        meta: { lane, carrierAsk, high, postedRate, midpoint: mid }
      };
      try { await createAlert(owner_alert); } catch {}
      await logEvent("draft_negotiate", { lane, step: "above_high", postedRate, low, high, mid, carrierAsk });
      return json(res, 200, {
        ok: true,
        result: {
          decision: "escalate",
          draft_reply_text: "",
          reasoning: `Carrier ask $${carrierAsk} above RateView high $${high}. Owner must handle.`,
          owner_alert
        }
      });
    }

    // After midpoint: if silence flag is set, alert owner
    if (stage === "midpoint_sent" && noResponseAfterMidpoint) {
      const lastAsk = carrierAsk;
      const within100 = lastAsk != null ? (Math.abs(lastAsk - mid) <= 100) : false;

      const owner_alert = {
        severity: "high",
        title: "No response after midpoint",
        message: within100
          ? `Carrier last at $${lastAsk} for ${lane || "lane"} (midpoint $${mid}) and went silent after midpoint. Owner decision needed.`
          : `Carrier went silent after midpoint $${mid} for ${lane || "lane"}. Owner decision needed.`,
        meta: { lane, postedRate, midpoint: mid, carrierAsk: lastAsk ?? null }
      };

      try { await createAlert(owner_alert); } catch {}
      await logEvent("draft_negotiate", { lane, step: "silent_after_midpoint", postedRate, low, high, mid, carrierAsk: lastAsk ?? null });

      return json(res, 200, {
        ok: true,
        result: {
          decision: "owner_alert",
          draft_reply_text: "",
          reasoning: "No response after midpoint; alerted owner.",
          owner_alert
        }
      });
    }

    // 1) First response when questioned about rate (no number)
    if ((carrierAsk == null) && looksLikeRateRequest(emailText)) {
      const quote = (low != null && low > 0) ? low : postedRate;

      await logEvent("draft_negotiate", { lane, step: "quote_low", quote, postedRate, low, high, mid });

      return json(res, 200, {
        ok: true,
        result: {
          decision: "quote_low",
          quote_rate: quote,
          draft_reply_text: msgAround(quote),
          reasoning: `Carrier asked for rate with no number. Respond around RateView low ($${quote}).`,
          next_stage: "initial_sent"
        }
      });
    }

    // If no ask and not obviously a "rate?" email
    if (carrierAsk == null) {
      await logEvent("draft_negotiate", { lane, step: "ask_rate", postedRate, low, high, mid });
      return json(res, 200, {
        ok: true,
        result: {
          decision: "ask_rate",
          draft_reply_text: "What rate are you looking for?",
          reasoning: "No carrier ask found. Request their rate.",
          next_stage: stage || ""
        }
      });
    }

    // If carrier is at or below midpoint at any time: lock it
    if (carrierAsk <= mid) {
      await logEvent("draft_negotiate", { lane, step: "accept_mid_or_below", postedRate, low, high, mid, carrierAsk });
      return json(res, 200, {
        ok: true,
        result: {
          decision: "accept",
          draft_reply_text: `Let’s do it at $${carrierAsk}.`,
          reasoning: `Carrier is at/below midpoint (carrier $${carrierAsk}, midpoint $${mid}). Lock it.`,
          next_stage: "done"
        }
      });
    }

    // 2) First counter above midpoint: "We could do $NEXT."
    if (stage !== "next_offer_sent" && stage !== "midpoint_sent") {
      const nextOffer = computeNextOffer({ postedRate, carrierAsk, mid });

      // alert if within $100 of midpoint
      if (Math.abs(carrierAsk - mid) <= 100) {
        try {
          await createAlert({
            severity: "high",
            title: "Carrier offer near midpoint",
            message: `Carrier at $${carrierAsk} for ${lane || "lane"} (midpoint $${mid}). You may want to step in after midpoint.`,
            meta: { lane, carrierAsk, midpoint: mid, postedRate }
          });
        } catch {}
      }

      await logEvent("draft_negotiate", { lane, step: "next_offer", postedRate, low, high, mid, carrierAsk, nextOffer });

      return json(res, 200, {
        ok: true,
        result: {
          decision: "counter",
          counter_rate: nextOffer,
          draft_reply_text: msgCouldDo(nextOffer),
          reasoning: `Carrier countered above midpoint. Respond with next offer ($${nextOffer}).`,
          next_stage: "next_offer_sent"
        }
      });
    }

    // 3) Last counter: midpoint
    await logEvent("draft_negotiate", { lane, step: "midpoint_best", postedRate, low, high, mid, carrierAsk });

    return json(res, 200, {
      ok: true,
      result: {
        decision: "final_midpoint",
        counter_rate: mid,
        draft_reply_text: msgBestIs(mid),
        reasoning: `Final position is midpoint $${mid}.`,
        next_stage: "midpoint_sent"
      }
    });
  }

  await logEvent("unknown_route", { path: req.url, method: req.method });
  return json(res, 404, { error: "Not found" });
});

server.listen(10000, () => console.log("Server running (port 10000)"));
