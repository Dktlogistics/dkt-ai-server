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
// OpenAI helper (JSON only) - used for /draft/carrier-reply
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
// Helpers
// --------------------
function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

function numOrNull(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

function roundTo5(n) {
  return Math.round(n / 5) * 5;
}

function ceilTo25(n) {
  return Math.ceil(n / 25) * 25;
}

function midpointFromLowHigh(low, high) {
  const l = numOrNull(low);
  const h = numOrNull(high);
  if (l == null || h == null) return null;
  // if low/high accidentally come in as 0/0, treat as invalid
  if (l <= 0 || h <= 0) return null;
  return Math.round((l + h) / 2);
}

function parseRateFromText(text) {
  const t = String(text || "");
  const m1 = t.match(/\$\s*([0-9]{3,5})(?:\.[0-9]{1,2})?/);
  if (m1) return numOrNull(m1[1]);
  const m2 = t.match(/\b([0-9]{3,5})\b/);
  if (m2) return numOrNull(m2[1]);
  return null;
}

// Allowlist load_context keys (guardrail)
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

function formatPrice(rate) {
  return `We’re at $${rate}.`;
}

function computeFirstCounter({ postedRate, carrierAsk, mid }) {
  // posted + 35% of gap, bump clamped 60..120, rounded to 5, never above midpoint
  const gap = carrierAsk - postedRate;
  let bump = gap * 0.35;
  bump = Math.max(60, Math.min(120, bump));
  let counter = postedRate + bump;
  counter = roundTo5(counter);
  counter = Math.min(counter, mid);
  return Math.round(counter);
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

  // Health
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

  // Alerts
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
SAFETY MODE: ${safetyMode}. Draft only.

Hard requirements:
- Format EXACTLY:
  1) Picks up {pickup_fcfs_or_appt} {pickup_time_window_military} in {pickup_city_state}
  2) Delivers to {delivery_city_state} {delivery_fcfs_or_appt} {delivery_time_window_military}
  3) Truckload of {commodity_desc} weighing {weight_lbs}lbs
- Ask for MC ONLY if not found.
- If load_context has details, DO NOT ask to confirm them.
- Keep tone polite, short.

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
      mc_found: result?.mc_found,
      used_load_context: haveAll
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

  // Draft negotiation (deterministic, firm, short)
  if (req.url === "/draft/negotiate" && req.method === "POST") {
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
    const lane = loadContext.lane_guess || null;

    const negotiationRound = Math.max(1, Number(body.negotiation_round || 1));
    const lastCounter = numOrNull(body.last_counter_rate);

    const postedRate = numOrNull(loadContext.posted_rate);
    const low = numOrNull(loadContext.rateview_low);
    const high = numOrNull(loadContext.rateview_high);
    const mid =
      numOrNull(loadContext.midpoint_rate) ??
      midpointFromLowHigh(low, high);

    let carrierAsk = numOrNull(body.carrier_ask_rate);
    if (carrierAsk == null) carrierAsk = parseRateFromText(emailText);

    // Required inputs (prevents midpoint=0 bugs)
    if (postedRate == null || postedRate <= 0) {
      await logEvent("draft_negotiate_error", { reason: "missing_posted_rate" });
      return json(res, 400, { error: "Missing/invalid load_context.posted_rate" });
    }
    if (mid == null || mid <= 0) {
      await logEvent("draft_negotiate_error", { reason: "missing_midpoint", low, high, midpoint_rate: loadContext.midpoint_rate });
      return json(res, 400, { error: "Missing/invalid midpoint. Provide midpoint_rate OR rateview_low & rateview_high." });
    }

    // If no ask found, ask for it (short)
    if (carrierAsk == null) {
      const result = {
        decision: "counter",
        counter_rate: null,
        midpoint_rate: mid,
        max_rate_without_owner: high ?? null,
        draft_reply_text: "What rate are you looking for?",
        reasoning: "Carrier ask rate not provided/found. Request it.",
        owner_alert: null
      };
      await logEvent("draft_negotiate", { lane, postedRate, mid, high, carrierAsk: null, decision: "ask_rate" });
      return json(res, 200, { ok: true, result });
    }

    // Flag if carrier ask is within $100 of midpoint (your instruction)
    if (Math.abs(carrierAsk - mid) <= 100) {
      try {
        await createAlert({
          severity: "high",
          title: "Carrier offer near midpoint",
          message: `Carrier asked $${carrierAsk} for ${lane || "lane"} (midpoint ~$${mid}). Owner should review/accept.`,
          meta: { lane, carrierAsk, midpoint: mid, postedRate }
        });
      } catch {}
    }

    // Above high => escalate
    if (high != null && high > 0 && carrierAsk > high) {
      const result = {
        decision: "escalate",
        counter_rate: null,
        midpoint_rate: mid,
        max_rate_without_owner: high,
        draft_reply_text: "",
        reasoning: `Carrier ask $${carrierAsk} is above RateView high $${high}. Owner should handle.`,
        owner_alert: {
          severity: "high",
          title: "Carrier ask above high",
          message: `Carrier asked $${carrierAsk} (above high $${high}) for ${lane || "lane"}.`,
          meta: { lane, carrierAsk, high, postedRate, midpoint: mid }
        }
      };
      try {
        await createAlert(result.owner_alert);
      } catch {}
      await logEvent("draft_negotiate", { lane, postedRate, mid, high, carrierAsk, decision: "escalate" });
      return json(res, 200, { ok: true, result });
    }

    // Round behavior you requested:
    // Round 1 = first counter (e.g., 685)
    // Round 2 = snap to next 25-grid above (first+25 => 725 style)
    // Round 3+ = MIDPOINT (final)
    const firstCounter = computeFirstCounter({ postedRate, carrierAsk, mid });

    let counter = firstCounter;

    if (negotiationRound === 2) {
      const base = lastCounter != null ? lastCounter : firstCounter;
      counter = ceilTo25(base + 25); // 685 -> 710 -> 725
      counter = Math.min(counter, mid);
      counter = roundTo5(counter);
    }

    if (negotiationRound >= 3) {
      counter = mid; // your instruction: round 3 = midpoint
    }

    // Decision
    let decision = "counter";
    if (carrierAsk <= counter) decision = "accept";

    const result = {
      decision,
      counter_rate: counter,
      midpoint_rate: mid,
      max_rate_without_owner: high ?? null,
      draft_reply_text: formatPrice(counter),
      reasoning: `Posted $${postedRate}, carrier asked $${carrierAsk}, midpoint $${mid}. Round ${negotiationRound} => $${counter}.`,
      owner_alert: null
    };

    // If at midpoint and carrier still above, flag owner
    if (counter === mid && carrierAsk > mid) {
      result.decision = "escalate";
      result.owner_alert = {
        severity: "high",
        title: "Negotiation at midpoint",
        message: `At midpoint $${mid} for ${lane || "lane"} but carrier still at $${carrierAsk}. Owner decision needed.`,
        meta: { lane, postedRate, midpoint: mid, carrierAsk }
      };
      try {
        await createAlert(result.owner_alert);
      } catch {}
    }

    await logEvent("draft_negotiate", {
      lane,
      postedRate,
      mid,
      high,
      carrierAsk,
      negotiationRound,
      lastCounter,
      counter,
      decision: result.decision
    });

    return json(res, 200, { ok: true, result });
  }

  await logEvent("unknown_route", { path: req.url, method: req.method });
  return json(res, 200, { status: "DKT AI server running (secured)" });
});

server.listen(10000, () => console.log("Server running (port 10000)"));
