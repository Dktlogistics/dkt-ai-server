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
// OpenAI
// --------------------
async function openaiChatJSON({ system, user, maxTokens = 500 }) {
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

  // Classify email
  if (req.url === "/classify-email" && req.method === "POST") {
    const bodyText = await readBody(req);
    let body;
    try {
      body = JSON.parse(bodyText || "{}");
    } catch {
      await logEvent("classify_email_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    const emailText = (body.email_text || "").toString().slice(0, 8000);
    if (!emailText) return json(res, 400, { error: "Missing email_text" });

    const system =
      "You classify logistics emails. Output ONLY valid JSON with keys: category (carrier|customer|spam|other), confidence (0-1), reason.";
    const user = `Classify this email:\n\n${emailText}`;

    const result = await openaiChatJSON({ system, user, maxTokens: 200 });
    await logEvent("classify_email", { result });
    return json(res, 200, result);
  }

  // NEW: Draft a carrier reply (Phase 1, draft-only)
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

    const emailText = (body.email_text || "").toString().slice(0, 8000);
    if (!emailText) return json(res, 400, { error: "Missing email_text" });

    // We do NOT access TAI/DAT/Highway yet in Phase 1.
    // We only draft a reply in your format and identify missing MC.
    const system = `
You are DKT Logistics' carrier email assistant.
You MUST follow the rules provided. You are in SAFETY MODE: ${safetyMode}.
You are NOT allowed to send emails, post loads, or change any system. Draft only.

Output ONLY valid JSON with keys:
- lane_guess (string)
- mc_found (boolean)
- mc_value (string|null)
- needs_mc_request (boolean)
- draft_reply_text (string)  // must follow template format from rules.email_handling.template_format + details_line
- negotiation_next_line (string) // short suggested next line if carrier asks rate or offers high
- notes_for_owner (string) // short bullet style
`;

    const user = `
RULES (JSON):
${JSON.stringify(rules)}

INCOMING EMAIL TEXT:
${emailText}

TASK:
1) Guess lane from email (like "Seymour IN to Cookeville TN") if present.
2) Detect MC/DOT if present.
3) Produce a draft reply using the exact template format in rules.email_handling.template_format.
   Since Phase 1 has no TAI access, if pickup/delivery times/commodity/weight are unknown, draft reply should:
   - ask one clarifying question: "Can you confirm pickup/delivery times, weight, and commodity?" (keep it short)
   - still ask MC if missing.
4) Keep tone polite and not overly formal.
Return ONLY JSON.
`;

    const result = await openaiChatJSON({ system, user, maxTokens: 700 });

    await logEvent("draft_carrier_reply", {
      lane_guess: result?.lane_guess,
      mc_found: result?.mc_found,
      needs_mc_request: result?.needs_mc_request
    });

    return json(res, 200, { ok: true, result });
  }

  await logEvent("unknown_route", { path: req.url, method: req.method });
  return json(res, 200, { status: "DKT AI server running (secured)" });
});

server.listen(10000, () => console.log("Server running on port 10000"));
