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
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

// --------------------
// OpenAI helper (JSON only)
// --------------------
async function openaiChatJSON({ system, user, maxTokens = 800 }) {
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

function isNonEmptyString(x) {
  return typeof x === "string" && x.trim().length > 0;
}

function normalizeLoadContext(ctx = {}) {
  // Only allow expected keys (guardrail)
  const safe = {
    pickup_fcfs_or_appt: ctx.pickup_fcfs_or_appt ?? null,
    pickup_time_window_military: ctx.pickup_time_window_military ?? null,
    pickup_city_state: ctx.pickup_city_state ?? null,
    delivery_fcfs_or_appt: ctx.delivery_fcfs_or_appt ?? null,
    delivery_time_window_military: ctx.delivery_time_window_military ?? null,
    delivery_city_state: ctx.delivery_city_state ?? null,
    weight_lbs: ctx.weight_lbs ?? null,
    commodity_desc: ctx.commodity_desc ?? null,
    posted_rate: ctx.posted_rate ?? null
  };

  // Basic cleanup
  for (const k of Object.keys(safe)) {
    if (typeof safe[k] === "string") safe[k] = safe[k].trim();
  }
  return safe;
}

let tablesReady = false;

const server = http.createServer(async (req, res) => {
  // --------------------
  // AUTH (locked server)
  // --------------------
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

  // Save rules config
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

  // Read rules config
  if (req.url === "/config/rules" && req.method === "GET") {
    const rules = await getConfig("rules");
    await logEvent("config_rules_read", {});
    return json(res, 200, { rules });
  }

  // Draft a carrier reply (Phase 1, draft-only)
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
    const haveAllLoadDetails =
      isNonEmptyString(loadContext.pickup_fcfs_or_appt) &&
      isNonEmptyString(loadContext.pickup_time_window_military) &&
      isNonEmptyString(loadContext.pickup_city_state) &&
      isNonEmptyString(loadContext.delivery_fcfs_or_appt) &&
      isNonEmptyString(loadContext.delivery_time_window_military) &&
      isNonEmptyString(loadContext.delivery_city_state) &&
      (typeof loadContext.weight_lbs === "number" || isNonEmptyString(String(loadContext.weight_lbs || ""))) &&
      isNonEmptyString(loadContext.commodity_desc);

    const system = `
You are DKT Logistics' carrier email assistant.
You MUST follow the rules provided.
You are in SAFETY MODE: ${safetyMode}.
You are NOT allowed to send emails, post loads, or change any system. Draft only.

Hard requirements:
- Format the load details EXACTLY like DKT wants:
  Line 1: "Picks up {pickup_fcfs_or_appt} {pickup_time_window_military} in {pickup_city_state}"
  Line 2: "Delivers to {delivery_city_state} {delivery_fcfs_or_appt} {delivery_time_window_military}"
  Line 3: "Truckload of {commodity_desc} weighing {weight_lbs}lbs"
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
1) Guess lane from email if present (e.g., "Seymour IN to Cookeville TN").
2) Detect MC/DOT in the email if present. If none found, set mc_found=false and needs_mc_request=true.
3) Draft reply using the 3-line required format. Use LOAD_CONTEXT values if provided.
4) If LOAD_CONTEXT is missing key details, ask ONE short clarifying question:
   "Can you confirm pickup/delivery times, weight, and commodity?"
5) If MC is missing, include: "${rules?.email_handling?.ask_mc_if_missing || "What is your MC number?"}"
6) Provide a short negotiation_next_line aligned with DKT strategy.
Return ONLY JSON.
`;

    const result = await openaiChatJSON({ system, user, maxTokens: 900 });

    await logEvent("draft_carrier_reply", {
      lane_guess: result?.lane_guess,
      mc_found: result?.mc_found,
      used_load_context: haveAllLoadDetails
    });

    return json(res, 200, { ok: true, result });
  }

  await logEvent("unknown_route", { path: req.url, method: req.method });
  return json(res, 200, { status: "DKT AI server running (secured)" });
});

server.listen(10000, () => console.log("Server running on port 10000"));
