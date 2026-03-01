const http = require("http");
const { Pool } = require("pg");

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

// --------------------
// OpenAI helper (JSON only)
// --------------------
async function openaiChatJSON({ system, user, maxTokens = 900 }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) return { error: "OPENAI_API_KEY not set" };

  const payload = {
    model: "gpt-4.1-mini",
    messages: [
      { role: "system", content: system },
      { role: "user", content: user }
    ],
    temperature: 0,
    max_tokens: maxTokens
  };

  const resp = await fetch("https://api.openai.com/v1/chat/completions", {
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

  try {
    return JSON.parse(content);
  } catch {
    return { error: "Model returned non-JSON", raw: content };
  }
}

// --------------------
// Draft helpers
// --------------------
function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
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

function hasAllLoadDetails(loadContext) {
  return (
    isNonEmptyString(loadContext.pickup_fcfs_or_appt) &&
    isNonEmptyString(loadContext.pickup_time_window_military) &&
    isNonEmptyString(loadContext.pickup_city_state) &&
    isNonEmptyString(loadContext.delivery_fcfs_or_appt) &&
    isNonEmptyString(loadContext.delivery_time_window_military) &&
    isNonEmptyString(loadContext.delivery_city_state) &&
    (typeof loadContext.weight_lbs === "number" ||
      isNonEmptyString(String(loadContext.weight_lbs || ""))) &&
    isNonEmptyString(loadContext.commodity_desc)
  );
}

function numOrNull(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function midpoint(low, high) {
  const l = numOrNull(low);
  const h = numOrNull(high);
  if (l == null || h == null) return null;
  return Math.round((l + h) / 2);
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

  // Initialize DB tables once per process
  if (!tablesReady) {
    try {
      await ensureTables();
      tablesReady = true;
      await logEvent("server_start", { ok: true });
    } catch (e) {
      return json(res, 500, { error: "DB init failed", details: e?.message || String(e) });
    }
  }

  // --------------------
  // ROUTES
  // --------------------
  if (req.url === "/health" && req.method === "GET") {
    await logEvent("health_check", {});
    return json(res, 200, { ok: true });
  }

  // Rules config
  if (req.url === "/config/rules" && req.method === "POST") {
    const bodyText = await readBody(req);
    let body;
    try {
      body = JSON.parse(bodyText || "{}");
    } catch {
      await logEvent("config_rules_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    await setConfig("rules", body);
    await logEvent("config_rules_saved", { keys: Object.keys(body || {}) });
    return json(res, 200, { ok: true });
  }

  if (req.url === "/config/rules" && req.method === "GET") {
    const rules = await getConfig("rules");
    await logEvent("config_rules_read", {});
    return json(res, 200, { rules });
  }

  // Owner alerts
  if (req.url === "/alerts" && req.method === "POST") {
    const bodyText = await readBody(req);
    let body;
    try {
      body = JSON.parse(bodyText || "{}");
    } catch {
      await logEvent("alerts_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    const severity = (body.severity || "info").toString();
    const title = (body.title || "").toString().slice(0, 140);
    const message = (body.message || "").toString().slice(0, 2000);
    const meta = body.meta && typeof body.meta === "object" ? body.meta : {};

    if (!title || !message) return json(res, 400, { error: "Missing title or message" });

    await createAlert({ severity, title, message, meta });
    await logEvent("alert_created", { severity, title });

    return json(res, 200, { ok: true });
  }

  if (req.url.startsWith("/alerts") && req.method === "GET") {
    const url = new URL(req.url, "https://dummy.local");
    const limit = url.searchParams.get("limit") || "25";
    const alerts = await listAlerts(limit);
    await logEvent("alerts_listed", { count: alerts.length });
    return json(res, 200, { ok: true, alerts });
  }

  // Draft carrier reply (Phase 1)
  if (req.url === "/draft/carrier-reply" && req.method === "POST") {
    const rules = (await getConfig("rules")) || {};
    const safetyMode = rules?.safety?.mode || "draft_only";

    const bodyText = await readBody(req);
    let body;
    try {
      body = JSON.parse(bodyText || "{}");
    } catch {
      await logEvent("draft_reply_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    const emailText = (body.email_text || "").toString().slice(0, 12000);
    if (!emailText) return json(res, 400, { error: "Missing email_text" });

    const loadContext = normalizeLoadContext(body.load_context || {});
    const haveAll = hasAllLoadDetails(loadContext);

    const system = `
You are DKT Logistics' carrier email assistant.
You MUST follow the rules provided.
You are in SAFETY MODE: ${safetyMode}.
You are NOT allowed to send emails, post loads, or change any system. Draft only.

Hard requirements:
- Format load details EXACTLY as:
  1) Picks up {pickup_fcfs_or_appt} {pickup_time_window_military} in {pickup_city_state}
  2) Delivers to {delivery_city_state} {delivery_fcfs_or_appt} {delivery_time_window_military}
  3) Truckload of {commodity_desc} weighing {weight_lbs}lbs
- Ask for MC ONLY if you did not find it in the email.
- If load_context contains the needed fields, DO NOT ask to confirm pickup/delivery times, weight, or commodity.
- Keep tone polite, short, not overly formal.

Output ONLY valid JSON with keys:
- lane_guess (string)
- mc_found (boolean)
- mc_value (string|null)
- needs_mc_request (boolean)
- draft_reply_text (string)
- negotiation_next_line (string)
- notes_for_owner (string)
`;

    const user = `
RULES (JSON):
${JSON.stringify(rules)}

INCOMING EMAIL TEXT:
${emailText}

LOAD_CONTEXT (may be empty; if present, trust it):
${JSON.stringify(loadContext)}

TASK:
1) Guess lane from email if present.
2) Detect MC/DOT in the email if present. If none found, set mc_found=false and needs_mc_request=true.
3) Draft reply using the 3-line required format. Use LOAD_CONTEXT values if provided.
4) If LOAD_CONTEXT is missing key details, ask ONE short clarifying question:
   "Can you confirm pickup/delivery times, weight, and commodity?"
5) If MC is missing, include: "${rules?.email_handling?.ask_mc_if_missing || "What is your MC number?"}"
6) Provide a short negotiation_next_line aligned with DKT strategy. Use posted_rate/midpoint_rate if provided.
Return ONLY JSON.
`;

    const result = await openaiChatJSON({ system, user, maxTokens: 950 });

    await logEvent("draft_carrier_reply", {
      lane_guess: result?.lane_guess,
      mc_found: result?.mc_found,
      used_load_context: haveAll
    });

    // Auto-alerts
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

  // NEW: Draft negotiation response (Phase 1, draft-only)
  if (req.url === "/draft/negotiate" && req.method === "POST") {
    const rules = (await getConfig("rules")) || {};
    const safetyMode = rules?.safety?.mode || "draft_only";

    const bodyText = await readBody(req);
    let body;
    try {
      body = JSON.parse(bodyText || "{}");
    } catch {
      await logEvent("draft_negotiate_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    const emailText = (body.email_text || "").toString().slice(0, 12000);
    const loadContext = normalizeLoadContext(body.load_context || {});
    const carrierAsk = numOrNull(body.carrier_ask_rate);
    const postedRate = numOrNull(loadContext.posted_rate);
    const low = numOrNull(loadContext.rateview_low);
    const high = numOrNull(loadContext.rateview_high);
    const mid = numOrNull(loadContext.midpoint_rate) ?? midpoint(low, high);

    // Guardrail: if we don't have at least posted rate OR midpoint, we can still draft, but we should flag.
    const system = `
You are DKT Logistics' carrier negotiation assistant.
SAFETY MODE: ${safetyMode}. Draft-only.

You MUST follow these guardrails:
- Never recommend paying above the RateView HIGH unless owner approves.
- Prefer booking below MIDPOINT when possible; MIDPOINT is acceptable.
- If carrier ask is far above posted rate, counter up but stay under midpoint if possible.
- Keep tone polite and personal. You may use a light-load/fuel-angle if weight under 25000 lbs.

Return ONLY valid JSON with:
- decision (accept|counter|escalate)
- counter_rate (number|null)
- max_rate_without_owner (number|null)   // typically = high
- draft_reply_text (string)             // what to send the carrier (draft)
- reasoning (string)                    // short explanation for owner
- owner_alert (object|null)             // if escalate, include {severity,title,message,meta}
`;

    const user = `
RULES JSON:
${JSON.stringify(rules)}

CONTEXT:
lane: ${loadContext.lane_guess || "(unknown)"}
weight_lbs: ${loadContext.weight_lbs ?? "(unknown)"}
posted_rate: ${postedRate ?? "(unknown)"}
rateview_low: ${low ?? "(unknown)"}
rateview_high: ${high ?? "(unknown)"}
midpoint_rate: ${mid ?? "(unknown)"}
carrier_ask_rate: ${carrierAsk ?? "(unknown)"}

CARRIER MESSAGE / EMAIL:
${emailText}

TASK:
- If carrier_ask_rate is not provided, infer it if the carrier states a rate in text; otherwise ask "What rate are you looking for?"
- Produce a decision and a single best draft reply.
- Enforce: never above HIGH without owner. If carrier asks above HIGH, decision must be escalate.
`;

    const result = await openaiChatJSON({ system, user, maxTokens: 900 });

    await logEvent("draft_negotiate", {
      lane: loadContext.lane_guess || null,
      postedRate,
      mid,
      high,
      carrierAsk
    });

    // If model suggests escalation, create an owner alert
    try {
      if (result?.owner_alert && result?.owner_alert?.title) {
        await createAlert({
          severity: result.owner_alert.severity || "high",
          title: result.owner_alert.title,
          message: result.owner_alert.message || "Negotiation requires owner review.",
          meta: result.owner_alert.meta || {}
        });
      }
    } catch {}

    return json(res, 200, { ok: true, result });
  }

  await logEvent("unknown_route", { path: req.url, method: req.method });
  return json(res, 200, { status: "DKT AI server running (secured)" });
});

server.listen(10000, () => console.log("Server running on port 10000"));
