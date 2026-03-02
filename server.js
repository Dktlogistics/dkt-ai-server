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

  // ✅ Designated lanes (Lovable-controlled)
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

      lane_key TEXT,
      notes TEXT,
      created_by TEXT
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS designated_lanes_active_lane_key_idx
    ON designated_lanes (lane_key)
    WHERE is_active = TRUE;
  `);

  // ✅ NEW: load operations queue (AI system of record; TAI remains system of record for booking)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS load_ops (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

      tai_load_id TEXT,              -- whatever identifier you have from TAI (read-only)
      lane_key TEXT NOT NULL,

      pickup_city_state TEXT,
      delivery_city_state TEXT,
      weight_lbs TEXT,
      commodity_desc TEXT,

      pickup_fcfs_or_appt TEXT,
      pickup_time_window_military TEXT,
      delivery_fcfs_or_appt TEXT,
      delivery_time_window_military TEXT,

      posted_rate NUMERIC,
      rateview_low NUMERIC,
      rateview_high NUMERIC,
      midpoint_rate NUMERIC,

      is_designated BOOLEAN NOT NULL DEFAULT FALSE,
      status TEXT NOT NULL DEFAULT 'new',  -- new|blocked_designated|blocked_missing_data|ready_to_post|posted_to_dat|negotiating|midpoint_sent|locked_in|stale_no_response|closed

      -- negotiation tracking
      negotiation_stage TEXT DEFAULT '',
      last_carrier_ask NUMERIC,
      last_outbound_offer NUMERIC,
      last_inbound_ts TIMESTAMPTZ,
      last_outbound_ts TIMESTAMPTZ,

      -- DAT tracking (scaffold)
      dat_post_id TEXT,
      dat_last_refresh_ts TIMESTAMPTZ,
      dat_refresh_count INT NOT NULL DEFAULT 0,

      -- misc
      meta JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS load_ops_status_idx
    ON load_ops (status);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS load_ops_lane_key_idx
    ON load_ops (lane_key);
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
// String / lane helpers
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
function splitCityState(cityStateStr) {
  const s = cleanStr(cityStateStr);
  if (!s) return { city: null, state: null };
  const m = s.match(/^(.+?)[,\s]+([A-Za-z]{2})$/);
  if (!m) return { city: s, state: null };
  return { city: m[1].trim(), state: m[2].toUpperCase() };
}

// --------------------
// ✅ Designated Lanes - DB helpers
// --------------------
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
  await pool.query(`UPDATE designated_lanes SET is_active = FALSE, updated_at = NOW() WHERE is_active = TRUE`);
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

async function isDesignatedLaneByLoadContext(loadContext) {
  const pick = splitCityState(loadContext?.pickup_city_state);
  const del = splitCityState(loadContext?.delivery_city_state);
  const laneKey = makeLaneKey(pick.city, pick.state, del.city, del.state);
  if (!laneKey) return { ok: false, designated: false, laneKey: null };
  const r = await pool.query(
    `SELECT id FROM designated_lanes
     WHERE is_active = TRUE AND lane_key = $1
     LIMIT 1`,
    [laneKey]
  );
  return { ok: true, designated: (r.rows?.length || 0) > 0, laneKey };
}

// --------------------
// Load Ops helpers
// --------------------
function nowISO() {
  return new Date().toISOString();
}

async function upsertLoadOpsFromIngest({ tai_load_id, load_context, meta = {} }) {
  const lc = normalizeLoadContext(load_context || {});
  const pick = splitCityState(lc.pickup_city_state);
  const del = splitCityState(lc.delivery_city_state);
  const laneKey = makeLaneKey(pick.city, pick.state, del.city, del.state);

  if (!laneKey) {
    return { ok: false, error: "Missing pickup/delivery city/state to build lane_key." };
  }

  const { designated } = await isDesignatedLaneByLoadContext(lc);

  const postedRate = numOrNull(lc.posted_rate);
  const low = numOrNull(lc.rateview_low);
  const high = numOrNull(lc.rateview_high);
  const mid = numOrNull(lc.midpoint_rate) ?? midpointFromLowHigh(low, high);

  const missingRequired =
    !cleanStr(lc.pickup_city_state) ||
    !cleanStr(lc.delivery_city_state) ||
    !cleanStr(lc.weight_lbs) ||
    !cleanStr(lc.commodity_desc);

  const missingRateView = (low == null || high == null || mid == null);

  let status = "new";
  if (designated) status = "blocked_designated";
  else if (missingRequired || postedRate == null || postedRate <= 0) status = "blocked_missing_data";
  else if (missingRateView) status = "blocked_missing_data";
  else status = "ready_to_post";

  const r = await pool.query(
    `
    INSERT INTO load_ops (
      tai_load_id, lane_key,
      pickup_city_state, delivery_city_state, weight_lbs, commodity_desc,
      pickup_fcfs_or_appt, pickup_time_window_military,
      delivery_fcfs_or_appt, delivery_time_window_military,
      posted_rate, rateview_low, rateview_high, midpoint_rate,
      is_designated, status, meta, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb,NOW())
    ON CONFLICT (lane_key) DO UPDATE SET
      tai_load_id = EXCLUDED.tai_load_id,
      pickup_city_state = EXCLUDED.pickup_city_state,
      delivery_city_state = EXCLUDED.delivery_city_state,
      weight_lbs = EXCLUDED.weight_lbs,
      commodity_desc = EXCLUDED.commodity_desc,
      pickup_fcfs_or_appt = EXCLUDED.pickup_fcfs_or_appt,
      pickup_time_window_military = EXCLUDED.pickup_time_window_military,
      delivery_fcfs_or_appt = EXCLUDED.delivery_fcfs_or_appt,
      delivery_time_window_military = EXCLUDED.delivery_time_window_military,
      posted_rate = EXCLUDED.posted_rate,
      rateview_low = EXCLUDED.rateview_low,
      rateview_high = EXCLUDED.rateview_high,
      midpoint_rate = EXCLUDED.midpoint_rate,
      is_designated = EXCLUDED.is_designated,
      status = EXCLUDED.status,
      meta = EXCLUDED.meta,
      updated_at = NOW()
    RETURNING id, lane_key, status, is_designated
    `,
    [
      cleanStr(tai_load_id),
      laneKey,
      cleanStr(lc.pickup_city_state),
      cleanStr(lc.delivery_city_state),
      cleanStr(lc.weight_lbs),
      cleanStr(lc.commodity_desc),
      cleanStr(lc.pickup_fcfs_or_appt),
      cleanStr(lc.pickup_time_window_military),
      cleanStr(lc.delivery_fcfs_or_appt),
      cleanStr(lc.delivery_time_window_military),
      postedRate,
      low,
      high,
      mid,
      designated,
      status,
      JSON.stringify(meta || {})
    ]
  );

  return { ok: true, record: r.rows?.[0], laneKey, designated, status };
}

// NOTE: we want lane_key unique-ish; simplest is to enforce uniqueness by index.
// We'll create it safely here.
async function ensureLoadOpsLaneKeyUnique() {
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_indexes WHERE indexname = 'load_ops_lane_key_unique'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX load_ops_lane_key_unique ON load_ops (lane_key)';
      END IF;
    END $$;
  `);
}

async function listActiveLoads({ limit = 100, status = null } = {}) {
  const lim = Math.max(1, Math.min(500, Number(limit) || 100));
  if (status) {
    const r = await pool.query(
      `SELECT *
       FROM load_ops
       WHERE status = $1
       ORDER BY id DESC
       LIMIT $2`,
      [String(status), lim]
    );
    return r.rows || [];
  }
  const r = await pool.query(
    `SELECT *
     FROM load_ops
     ORDER BY id DESC
     LIMIT $1`,
    [lim]
  );
  return r.rows || [];
}

async function updateLoadOps(id, patch = {}) {
  // Controlled updates only
  const fields = [];
  const vals = [];
  let i = 1;

  const allowed = new Set([
    "status",
    "negotiation_stage",
    "last_carrier_ask",
    "last_outbound_offer",
    "last_inbound_ts",
    "last_outbound_ts",
    "dat_post_id",
    "dat_last_refresh_ts",
    "dat_refresh_count",
    "meta"
  ]);

  for (const [k, v] of Object.entries(patch || {})) {
    if (!allowed.has(k)) continue;
    fields.push(`${k} = $${i++}`);
    if (k === "meta") vals.push(JSON.stringify(v || {}));
    else vals.push(v);
  }

  if (!fields.length) return;

  vals.push(Number(id));
  await pool.query(
    `UPDATE load_ops
     SET ${fields.join(", ")}, updated_at = NOW()
     WHERE id = $${i}`,
    vals
  );
}

async function getLoadByLaneKey(lane_key) {
  const r = await pool.query(
    `SELECT * FROM load_ops WHERE lane_key = $1 ORDER BY id DESC LIMIT 1`,
    [String(lane_key)]
  );
  return r.rows?.[0] || null;
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
// Core helpers
// --------------------
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
  const gap = carrierAsk - postedRate;
  let bump = gap * 0.35;
  bump = Math.max(60, Math.min(120, bump));
  let offer = postedRate + bump;
  offer = roundTo5(offer);
  offer = Math.min(offer, mid);
  return Math.round(offer);
}

// --------------------
// Safety + capability checks
// --------------------
function getSafety(rules) {
  const safety = rules?.safety || {};
  const caps = safety?.capabilities || {};
  return {
    live_actions_enabled: safety?.live_actions_enabled === true,
    email_send_enabled: caps?.email_send_enabled === true,
    dat_post_enabled: caps?.dat_post_enabled === true
  };
}

function requireLiveCapability(rules, capabilityName) {
  const safety = getSafety(rules);
  if (!safety.live_actions_enabled) return { ok: false, reason: "live_actions_disabled" };
  if (capabilityName === "email" && !safety.email_send_enabled) return { ok: false, reason: "email_send_disabled" };
  if (capabilityName === "dat" && !safety.dat_post_enabled) return { ok: false, reason: "dat_post_disabled" };
  return { ok: true };
}

// --------------------
// Live Email Sender (SendGrid via HTTP)
// ENV required:
//  SENDGRID_API_KEY
//  EMAIL_FROM
// --------------------
async function sendEmailSendGrid({ to, subject, text }) {
  if (!fetchFn) return { ok: false, error: "fetch not available" };

  const apiKey = process.env.SENDGRID_API_KEY;
  const from = process.env.EMAIL_FROM;

  if (!apiKey) return { ok: false, error: "SENDGRID_API_KEY not set" };
  if (!from) return { ok: false, error: "EMAIL_FROM not set" };
  if (!to) return { ok: false, error: "Missing 'to'" };

  const payload = {
    personalizations: [{ to: [{ email: to }], subject: subject || "" }],
    from: { email: from },
    content: [{ type: "text/plain", value: String(text || "") }]
  };

  const resp = await fetchFn("https://api.sendgrid.com/v3/mail/send", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  if (!resp.ok) {
    const errText = await resp.text();
    return { ok: false, error: `SendGrid error: ${resp.status} ${errText}` };
  }
  return { ok: true };
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
      await ensureLoadOpsLaneKeyUnique();
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
  // Rules config
  // --------------------
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

  // --------------------
  // Alerts
  // --------------------
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
    const inserted = await addDesignatedLane({
      pickup_city: b.pickup_city,
      pickup_state: b.pickup_state,
      delivery_city: b.delivery_city,
      delivery_state: b.delivery_state,
      lane_key: b.lane_key || null,
      notes: b.notes || "",
      created_by: b.created_by || "lovable"
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

  // --------------------
  // ✅ Load ingest + dashboard endpoints
  // --------------------
  // TAI (or Lovable) sends load details here. AI never writes to TAI.
  if (pathname === "/loads/ingest" && req.method === "POST") {
    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const b = parsed.value || {};
    const tai_load_id = b.tai_load_id || null;
    const load_context = b.load_context || {};
    const meta = b.meta || {};

    const r = await upsertLoadOpsFromIngest({ tai_load_id, load_context, meta });

    if (!r.ok) {
      await logEvent("load_ingest_failed", { error: r.error });
      return json(res, 400, { ok: false, error: r.error });
    }

    if (r.designated) {
      try {
        await createAlert({
          severity: "high",
          title: "Designated lane — load blocked",
          message: `Load ingested but blocked (designated lane).`,
          meta: { lane_key: r.laneKey, tai_load_id: cleanStr(tai_load_id) }
        });
      } catch {}
    } else if (r.status === "blocked_missing_data") {
      try {
        await createAlert({
          severity: "high",
          title: "Load blocked — missing data",
          message: `Load ingested but blocked (missing required fields or RateView).`,
          meta: { lane_key: r.laneKey, tai_load_id: cleanStr(tai_load_id) }
        });
      } catch {}
    }

    await logEvent("load_ingested", { tai_load_id: cleanStr(tai_load_id), lane_key: r.laneKey, status: r.status, designated: r.designated });
    return json(res, 200, { ok: true, result: r });
  }

  // Lovable dashboard
  if (pathname === "/loads/active" && req.method === "GET") {
    const limit = urlObj.searchParams.get("limit") || "100";
    const status = urlObj.searchParams.get("status") || null;
    const loads = await listActiveLoads({ limit, status });
    await logEvent("loads_active_listed", { count: loads.length, status: status || "all" });
    return json(res, 200, { ok: true, loads });
  }

  // --------------------
  // Draft carrier reply (still available, but production path should use live endpoints)
  // --------------------
  if (pathname === "/draft/carrier-reply" && req.method === "POST") {
    const rules = (await getConfig("rules")) || {};
    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const body = parsed.value;
    const emailText = (body.email_text || "").toString().slice(0, 12000);
    if (!emailText) return json(res, 400, { error: "Missing email_text" });

    const loadContext = normalizeLoadContext(body.load_context || {});
    const laneCheck = await isDesignatedLaneByLoadContext(loadContext);

    if (laneCheck.ok && laneCheck.designated) {
      try {
        await createAlert({
          severity: "high",
          title: "Designated lane — AI blocked",
          message: `Carrier email received on designated lane. AI will not respond.`,
          meta: { lane_key: laneCheck.laneKey, pickup: loadContext?.pickup_city_state, delivery: loadContext?.delivery_city_state }
        });
      } catch {}
      await logEvent("draft_carrier_reply_blocked_designated", { lane_key: laneCheck.laneKey });
      return json(res, 200, {
        ok: true,
        result: { blocked: true, reason: "designated_lane", draft_reply_text: "", notes_for_owner: "Designated lane — handle manually." }
      });
    }

    const system = `
You are DKT Logistics' carrier email assistant.

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

    await logEvent("draft_carrier_reply", { lane_guess: result?.lane_guess, mc_found: result?.mc_found });

    try {
      if (result?.needs_mc_request) {
        await createAlert({
          severity: "info",
          title: "MC needed",
          message: `Carrier did not provide MC.`,
          meta: { lane_guess: result?.lane_guess || null }
        });
      }
    } catch {}

    return json(res, 200, { ok: true, result });
  }

  // --------------------
  // Draft negotiate (same logic, but production should use /live/negotiate-and-send)
  // --------------------
  if (pathname === "/draft/negotiate" && req.method === "POST") {
    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const body = parsed.value;
    const emailText = (body.email_text || "").toString().slice(0, 12000);
    const loadContext = normalizeLoadContext(body.load_context || {});

    const laneCheck = await isDesignatedLaneByLoadContext(loadContext);
    if (laneCheck.ok && laneCheck.designated) {
      try {
        await createAlert({
          severity: "high",
          title: "Designated lane — negotiation blocked",
          message: `AI blocked negotiation attempt on designated lane.`,
          meta: { lane_key: laneCheck.laneKey }
        });
      } catch {}
      await logEvent("draft_negotiate_blocked_designated", { lane_key: laneCheck.laneKey });
      return json(res, 200, {
        ok: true,
        result: { decision: "blocked_designated_lane", draft_reply_text: "", reasoning: "Designated lane: owner-controlled; AI must not negotiate." }
      });
    }

    // Fields
    const postedRate = numOrNull(loadContext.posted_rate ?? body.posted_rate);
    const low = numOrNull(loadContext.rateview_low ?? body.rateview_low);
    const high = numOrNull(loadContext.rateview_high ?? body.rateview_high);
    const mid = numOrNull(loadContext.midpoint_rate ?? body.midpoint_rate) ?? midpointFromLowHigh(low, high);

    let carrierAsk = numOrNull(body.carrier_ask_rate);
    if (carrierAsk == null) carrierAsk = parseRateFromText(emailText);

    const stage = String(body.stage || "").toLowerCase();
    const noResponseAfterMidpoint = body.no_response_after_midpoint === true;

    if (postedRate == null || postedRate <= 0) return json(res, 400, { error: "Missing/invalid posted_rate" });
    if (mid == null || mid <= 0) return json(res, 400, { error: "Missing/invalid midpoint (need midpoint_rate or rateview_low/high)" });

    // Above high => owner alert only
    if (high != null && high > 0 && carrierAsk != null && carrierAsk > high) {
      const owner_alert = {
        severity: "high",
        title: "Carrier ask above high",
        message: `Carrier asked $${carrierAsk} (above high $${high}).`,
        meta: { carrierAsk, high, postedRate, midpoint: mid }
      };
      try { await createAlert(owner_alert); } catch {}
      await logEvent("draft_negotiate", { step: "above_high", postedRate, low, high, mid, carrierAsk });
      return json(res, 200, { ok: true, result: { decision: "escalate", draft_reply_text: "", reasoning: "Above high; owner must handle.", owner_alert } });
    }

    if (stage === "midpoint_sent" && noResponseAfterMidpoint) {
      const owner_alert = {
        severity: "high",
        title: "No response after midpoint",
        message: `Carrier went silent after midpoint $${mid}.`,
        meta: { postedRate, midpoint: mid, carrierAsk: carrierAsk ?? null }
      };
      try { await createAlert(owner_alert); } catch {}
      await logEvent("draft_negotiate", { step: "silent_after_midpoint", postedRate, low, high, mid, carrierAsk: carrierAsk ?? null });
      return json(res, 200, { ok: true, result: { decision: "owner_alert", draft_reply_text: "", reasoning: "No response after midpoint; alerted owner.", owner_alert } });
    }

    if ((carrierAsk == null) && looksLikeRateRequest(emailText)) {
      const quote = (low != null && low > 0) ? low : postedRate;
      await logEvent("draft_negotiate", { step: "quote_low", quote, postedRate, low, high, mid });
      return json(res, 200, { ok: true, result: { decision: "quote_low", quote_rate: quote, draft_reply_text: msgAround(quote), next_stage: "initial_sent" } });
    }

    if (carrierAsk == null) {
      await logEvent("draft_negotiate", { step: "ask_rate", postedRate, low, high, mid });
      return json(res, 200, { ok: true, result: { decision: "ask_rate", draft_reply_text: "What rate are you looking for?", next_stage: stage || "" } });
    }

    if (carrierAsk <= mid) {
      await logEvent("draft_negotiate", { step: "accept_mid_or_below", postedRate, low, high, mid, carrierAsk });
      return json(res, 200, { ok: true, result: { decision: "accept", draft_reply_text: `Let’s do it at $${carrierAsk}.`, next_stage: "done" } });
    }

    if (stage !== "next_offer_sent" && stage !== "midpoint_sent") {
      const nextOffer = computeNextOffer({ postedRate, carrierAsk, mid });
      if (Math.abs(carrierAsk - mid) <= 100) {
        try {
          await createAlert({
            severity: "high",
            title: "Carrier offer near midpoint",
            message: `Carrier at $${carrierAsk} (midpoint $${mid}).`,
            meta: { carrierAsk, midpoint: mid, postedRate }
          });
        } catch {}
      }
      await logEvent("draft_negotiate", { step: "next_offer", postedRate, low, high, mid, carrierAsk, nextOffer });
      return json(res, 200, { ok: true, result: { decision: "counter", counter_rate: nextOffer, draft_reply_text: msgCouldDo(nextOffer), next_stage: "next_offer_sent" } });
    }

    await logEvent("draft_negotiate", { step: "midpoint_best", postedRate, low, high, mid, carrierAsk });
    return json(res, 200, { ok: true, result: { decision: "final_midpoint", counter_rate: mid, draft_reply_text: msgBestIs(mid), next_stage: "midpoint_sent" } });
  }

  // --------------------
  // ✅ LIVE: send a real email (guardrails + audit)
  // Body:
  // { "to": "carrier@email.com", "subject": "...", "text": "...", "lane_key": "...", "load_context": {...} }
  // --------------------
  if (pathname === "/live/email/send" && req.method === "POST") {
    const rules = (await getConfig("rules")) || {};
    const cap = requireLiveCapability(rules, "email");
    if (!cap.ok) {
      await logEvent("live_email_blocked", { reason: cap.reason });
      return json(res, 403, { ok: false, error: "Live email disabled", reason: cap.reason });
    }

    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const b = parsed.value || {};
    const to = cleanStr(b.to);
    const subject = cleanStr(b.subject) || "";
    const text = cleanStr(b.text) || "";
    const loadContext = normalizeLoadContext(b.load_context || {});
    const lane_key = cleanStr(b.lane_key);

    if (!to || !text) return json(res, 400, { ok: false, error: "Missing to/text" });

    // Designated lane block
    let laneKeyFinal = lane_key;
    if (!laneKeyFinal) {
      const laneCheck = await isDesignatedLaneByLoadContext(loadContext);
      laneKeyFinal = laneCheck.laneKey || null;
      if (laneCheck.ok && laneCheck.designated) {
        try {
          await createAlert({
            severity: "high",
            title: "Designated lane — email blocked",
            message: "AI prevented sending email on designated lane.",
            meta: { lane_key: laneCheck.laneKey, to }
          });
        } catch {}
        await logEvent("live_email_blocked_designated", { lane_key: laneCheck.laneKey, to });
        return json(res, 200, { ok: true, blocked: true, reason: "designated_lane" });
      }
    }

    const sendRes = await sendEmailSendGrid({ to, subject, text });

    await logEvent("live_email_send_attempt", { ok: sendRes.ok, lane_key: laneKeyFinal, to, subject });

    if (!sendRes.ok) {
      try {
        await createAlert({
          severity: "high",
          title: "Live email failed",
          message: sendRes.error || "Unknown error",
          meta: { to, lane_key: laneKeyFinal }
        });
      } catch {}
      return json(res, 500, { ok: false, error: sendRes.error });
    }

    return json(res, 200, { ok: true });
  }

  // --------------------
  // ✅ LIVE: negotiate + send (this is your “autopilot” endpoint)
  //
  // Body:
  // {
  //   "lane_key": "seymour,IN->cookeville,TN",  (preferred)
  //   "email_from": "carrier@email.com",
  //   "email_text": "Can you do $900?",
  //   "stage": "next_offer_sent" | "midpoint_sent" | ...
  // }
  //
  // It will:
  //  - load load_ops by lane_key
  //  - run negotiation
  //  - if decision needs message => send live email automatically
  //  - update load_ops stage timestamps
  //  - if "Let’s do it" => owner alert to finalize in TAI
  //  - if carrier stops responding -> /jobs/tick handles it
  // --------------------
  if (pathname === "/live/negotiate-and-send" && req.method === "POST") {
    const rules = (await getConfig("rules")) || {};

    const cap = requireLiveCapability(rules, "email");
    if (!cap.ok) {
      await logEvent("live_negotiate_blocked", { reason: cap.reason });
      return json(res, 403, { ok: false, error: "Live actions disabled", reason: cap.reason });
    }

    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const b = parsed.value || {};
    const lane_key = cleanStr(b.lane_key);
    const email_from = cleanStr(b.email_from);
    const email_text = (b.email_text || "").toString().slice(0, 12000);
    const stage = String(b.stage || "").toLowerCase();

    if (!lane_key || !email_from || !email_text) return json(res, 400, { ok: false, error: "Missing lane_key/email_from/email_text" });

    const load = await getLoadByLaneKey(lane_key);
    if (!load) return json(res, 404, { ok: false, error: "No load found for lane_key. Ingest it first." });

    if (load.is_designated) {
      try {
        await createAlert({
          severity: "high",
          title: "Designated lane — autopilot blocked",
          message: "Carrier message received but AI is blocked on this lane.",
          meta: { lane_key, email_from }
        });
      } catch {}
      await logEvent("live_negotiate_blocked_designated", { lane_key, email_from });
      return json(res, 200, { ok: true, blocked: true, reason: "designated_lane" });
    }

    // validate RateView required
    const postedRate = numOrNull(load.posted_rate);
    const low = numOrNull(load.rateview_low);
    const high = numOrNull(load.rateview_high);
    const mid = numOrNull(load.midpoint_rate) ?? midpointFromLowHigh(low, high);

    if (!postedRate || !low || !high || !mid) {
      try {
        await createAlert({
          severity: "high",
          title: "Autopilot blocked — missing RateView",
          message: "Load missing posted/low/high/mid; AI cannot run live.",
          meta: { lane_key }
        });
      } catch {}
      await updateLoadOps(load.id, { status: "blocked_missing_data" });
      await logEvent("live_negotiate_blocked_missing_data", { lane_key });
      return json(res, 400, { ok: false, error: "Missing required RateView/posted rates for live autopilot." });
    }

    // parse carrier ask
    let carrierAsk = parseRateFromText(email_text);

    // above high => owner alert only, no email
    if (carrierAsk != null && carrierAsk > high) {
      try {
        await createAlert({
          severity: "high",
          title: "Carrier ask above high",
          message: `Carrier asked $${carrierAsk} (above high $${high}). Owner handle.`,
          meta: { lane_key, carrierAsk, high }
        });
      } catch {}
      await updateLoadOps(load.id, { status: "negotiating", last_carrier_ask: carrierAsk, last_inbound_ts: new Date().toISOString() });
      await logEvent("live_negotiate_escalate_above_high", { lane_key, carrierAsk, high });
      return json(res, 200, { ok: true, decision: "escalate", sent: false });
    }

    // decision logic (same as draft)
    let decision = null;
    let replyText = "";
    let nextStage = stage || "";

    if (carrierAsk == null && looksLikeRateRequest(email_text)) {
      decision = "quote_low";
      replyText = msgAround(low);
      nextStage = "initial_sent";
    } else if (carrierAsk == null) {
      decision = "ask_rate";
      replyText = "What rate are you looking for?";
      nextStage = stage || "";
    } else if (carrierAsk <= mid) {
      decision = "accept";
      replyText = `Let’s do it at $${carrierAsk}.`;
      nextStage = "done";
    } else if (stage !== "next_offer_sent" && stage !== "midpoint_sent") {
      decision = "counter";
      const nextOffer = computeNextOffer({ postedRate, carrierAsk, mid });
      replyText = msgCouldDo(nextOffer);
      nextStage = "next_offer_sent";
    } else {
      decision = "final_midpoint";
      replyText = msgBestIs(mid);
      nextStage = "midpoint_sent";
    }

    // Send live email
    const sendRes = await sendEmailSendGrid({
      to: email_from,
      subject: "", // keep blank unless you want to set a standard subject format
      text: replyText
    });

    await logEvent("live_negotiate_send_attempt", { ok: sendRes.ok, lane_key, decision, nextStage, carrierAsk });

    if (!sendRes.ok) {
      try {
        await createAlert({
          severity: "high",
          title: "Live negotiation email failed",
          message: sendRes.error || "Unknown error",
          meta: { lane_key, email_from, decision }
        });
      } catch {}
      return json(res, 500, { ok: false, error: sendRes.error });
    }

    // Update load state
    const patch = {
      status: (nextStage === "midpoint_sent") ? "midpoint_sent" : (nextStage === "done" ? "locked_in" : "negotiating"),
      negotiation_stage: nextStage,
      last_inbound_ts: new Date().toISOString(),
      last_outbound_ts: new Date().toISOString(),
      last_carrier_ask: carrierAsk ?? null
    };

    // If we sent a numeric offer, store it
    const offered = parseRateFromText(replyText);
    if (offered != null) patch.last_outbound_offer = offered;

    await updateLoadOps(load.id, patch);

    // If locked in -> owner alert to finalize in TAI
    if (decision === "accept") {
      try {
        await createAlert({
          severity: "high",
          title: "Load locked in",
          message: `“Let’s do it…” sent. Finalize booking in TAI.`,
          meta: { lane_key, tai_load_id: load.tai_load_id || null, agreed_rate: carrierAsk }
        });
      } catch {}
    }

    return json(res, 200, { ok: true, sent: true, decision, replyText, nextStage });
  }

  // --------------------
  // ✅ LIVE: DAT posting (scaffold)
  // This endpoint changes DB state and logs. Actual DAT API call is configurable later.
  //
  // Body: { "lane_key": "...", "dry_run": true|false }
  // --------------------
  if (pathname === "/live/dat/post" && req.method === "POST") {
    const rules = (await getConfig("rules")) || {};
    const cap = requireLiveCapability(rules, "dat");
    if (!cap.ok) {
      await logEvent("live_dat_post_blocked", { reason: cap.reason });
      return json(res, 403, { ok: false, error: "DAT posting disabled", reason: cap.reason });
    }

    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const b = parsed.value || {};
    const lane_key = cleanStr(b.lane_key);
    const dry_run = b.dry_run === true;

    if (!lane_key) return json(res, 400, { ok: false, error: "Missing lane_key" });

    const load = await getLoadByLaneKey(lane_key);
    if (!load) return json(res, 404, { ok: false, error: "No load found for lane_key. Ingest it first." });

    if (load.is_designated) {
      try {
        await createAlert({
          severity: "high",
          title: "Designated lane — DAT blocked",
          message: "AI prevented DAT post on designated lane.",
          meta: { lane_key }
        });
      } catch {}
      await logEvent("live_dat_post_blocked_designated", { lane_key });
      return json(res, 200, { ok: true, blocked: true, reason: "designated_lane" });
    }

    // required fields check
    const requiredOk =
      cleanStr(load.pickup_city_state) &&
      cleanStr(load.delivery_city_state) &&
      numOrNull(load.posted_rate) &&
      numOrNull(load.rateview_low) &&
      numOrNull(load.rateview_high) &&
      numOrNull(load.midpoint_rate);

    if (!requiredOk) {
      await updateLoadOps(load.id, { status: "blocked_missing_data" });
      try {
        await createAlert({
          severity: "high",
          title: "DAT post blocked — missing data",
          message: "Load missing required fields or RateView.",
          meta: { lane_key }
        });
      } catch {}
      await logEvent("live_dat_post_blocked_missing_data", { lane_key });
      return json(res, 400, { ok: false, error: "Missing required load data/RateView." });
    }

    // ✅ Actual DAT API call is intentionally abstracted until you plug in DAT credentials + endpoint.
    // For now, we generate a safe placeholder ID in non-dry-run so state machines work.
    const fakeDatPostId = `dat_${Date.now()}`;

    if (!dry_run) {
      await updateLoadOps(load.id, {
        status: "posted_to_dat",
        dat_post_id: fakeDatPostId,
        dat_last_refresh_ts: new Date().toISOString(),
        dat_refresh_count: 0
      });
    }

    await logEvent("live_dat_post", { lane_key, dry_run, dat_post_id: dry_run ? null : fakeDatPostId });

    return json(res, 200, { ok: true, dry_run, dat_post_id: dry_run ? null : fakeDatPostId });
  }

  // --------------------
  // ✅ LIVE: DAT update/refresh (scaffold)
  // Body: { "lane_key": "...", "new_rate": 700, "dry_run": true|false }
  // --------------------
  if (pathname === "/live/dat/update" && req.method === "POST") {
    const rules = (await getConfig("rules")) || {};
    const cap = requireLiveCapability(rules, "dat");
    if (!cap.ok) return json(res, 403, { ok: false, error: "DAT updates disabled", reason: cap.reason });

    const bodyText = await readBody(req);
    const parsed = safeJsonParse(bodyText);
    if (!parsed.ok) return json(res, 400, { error: "Invalid JSON body" });

    const b = parsed.value || {};
    const lane_key = cleanStr(b.lane_key);
    const new_rate = numOrNull(b.new_rate);
    const dry_run = b.dry_run === true;

    if (!lane_key || new_rate == null) return json(res, 400, { ok: false, error: "Missing lane_key/new_rate" });

    const load = await getLoadByLaneKey(lane_key);
    if (!load) return json(res, 404, { ok: false, error: "No load found for lane_key" });

    if (!load.dat_post_id) return json(res, 400, { ok: false, error: "Load has no dat_post_id yet (post first)." });

    // scaffold: update DB refresh bookkeeping
    if (!dry_run) {
      await updateLoadOps(load.id, {
        dat_last_refresh_ts: new Date().toISOString(),
        dat_refresh_count: Number(load.dat_refresh_count || 0) + 1
      });
    }

    await logEvent("live_dat_update", { lane_key, new_rate, dry_run, dat_post_id: load.dat_post_id });

    return json(res, 200, { ok: true, dry_run, dat_post_id: load.dat_post_id });
  }

  // --------------------
  // ✅ JOB TICK: run scheduled tasks (call this from Render Cron, Uptimerobot, or Lovable)
  // - detect midpoint silence and alert
  // - refresh DAT posts (stub hook)
  //
  // Query params:
  //   ?max=25
  // --------------------
  if (pathname === "/jobs/tick" && req.method === "POST") {
    const max = Math.max(1, Math.min(200, Number(urlObj.searchParams.get("max") || 25)));

    const rules = (await getConfig("rules")) || {};
    const silenceMinutes = Number(rules?.negotiation?.silence_minutes_after_midpoint || 60); // default 60 minutes
    const silenceMs = Math.max(5, silenceMinutes) * 60 * 1000;

    // 1) Midpoint silence check
    const r1 = await pool.query(
      `
      SELECT id, lane_key, tai_load_id, last_outbound_ts, negotiation_stage
      FROM load_ops
      WHERE status = 'midpoint_sent'
      ORDER BY id DESC
      LIMIT $1
      `,
      [max]
    );

    let silenceAlerts = 0;
    for (const row of r1.rows || []) {
      const lastOut = row.last_outbound_ts ? new Date(row.last_outbound_ts).getTime() : 0;
      if (!lastOut) continue;
      const age = Date.now() - lastOut;
      if (age >= silenceMs) {
        try {
          await createAlert({
            severity: "high",
            title: "Carrier silent after midpoint",
            message: `No response after midpoint. Check lane.`,
            meta: { lane_key: row.lane_key, tai_load_id: row.tai_load_id || null }
          });
        } catch {}
        await updateLoadOps(row.id, { status: "stale_no_response" });
        silenceAlerts++;
        await logEvent("job_midpoint_silence_alert", { lane_key: row.lane_key, tai_load_id: row.tai_load_id || null });
      }
    }

    // 2) DAT refresh hook (scaffold) — you’ll plug in your exact refresh/rate-bump rules here
    // Find posted loads that need refresh by time window rule.
    const r2 = await pool.query(
      `
      SELECT id, lane_key, dat_post_id, dat_last_refresh_ts, dat_refresh_count
      FROM load_ops
      WHERE status = 'posted_to_dat'
      ORDER BY id DESC
      LIMIT $1
      `,
      [max]
    );

    // For now we do nothing other than report how many are eligible.
    const datCandidates = (r2.rows || []).length;

    await logEvent("jobs_tick", { silenceAlerts, datCandidates, ts: nowISO() });

    return json(res, 200, { ok: true, silenceAlerts, datCandidates });
  }

  await logEvent("unknown_route", { path: req.url, method: req.method });
  return json(res, 404, { error: "Not found" });
});

server.listen(10000, () => console.log("Server running (port 10000)"));
