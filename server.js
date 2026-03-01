const http = require("http");
const { Pool } = require("pg");

// --------------------
// DB (Render provides DATABASE_URL)
// --------------------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Render Postgres commonly requires SSL. This setting is safe for hosted DBs.
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : undefined
});

async function ensureTables() {
  // Audit log table
  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      event_type TEXT NOT NULL,
      details JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);

  // App config table (stores your rules/settings as JSON)
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
    // Never crash the server if logging fails
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
    req.on("data", chunk => (data += chunk));
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

// --------------------
// OpenAI call (email classification only for now)
// --------------------
async function callOpenAIForClassification(text) {
  const apiKey = process.env.OPENAI_API_KEY;

  if (!apiKey) {
    return { category: "unknown", confidence: 0, reason: "OPENAI_API_KEY not set" };
  }

  const payload = {
    model: "gpt-4.1-mini",
    messages: [
      {
        role: "system",
        content:
          "You classify logistics emails. Output ONLY valid JSON with keys: category (carrier|customer|spam|other), confidence (0-1), reason."
      },
      {
        role: "user",
        content: `Classify this email:\n\n${text}`
      }
    ],
    temperature: 0
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
    return { category: "unknown", confidence: 0, reason: `OpenAI error: ${resp.status} ${errText}` };
  }

  const data = await resp.json();
  const content = data?.choices?.[0]?.message?.content?.trim() || "{}";

  try {
    return JSON.parse(content);
  } catch {
    return { category: "unknown", confidence: 0, reason: "Model returned non-JSON" };
  }
}

let tablesReady = false;

const server = http.createServer(async (req, res) => {
  // --------------------
  // AUTH (server locked)
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

  // Health check
  if (req.url === "/health" && req.method === "GET") {
    await logEvent("health_check", {});
    return json(res, 200, { ok: true });
  }

  // Save rules config (owner-only, secured)
  if (req.url === "/config/rules" && req.method === "POST") {
    const bodyText = await readBody(req);
    let body;
    try {
      body = JSON.parse(bodyText || "{}");
    } catch {
      await logEvent("config_rules_error", { reason: "invalid_json" });
      return json(res, 400, { error: "Invalid JSON body" });
    }

    // Store exactly what you send (no secrets)
    await setConfig("rules", body);
    await logEvent("config_rules_saved", { keys: Object.keys(body || {}) });

    return json(res, 200, { ok: true });
  }

  // Read rules config (owner-only, secured)
  if (req.url === "/config/rules" && req.method === "GET") {
    const rules = await getConfig("rules");
    await logEvent("config_rules_read", {});
    return json(res, 200, { rules });
  }

  // Classify email (Phase 1 safe feature)
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
    if (!emailText) {
      await logEvent("classify_email_error", { reason: "missing_email_text" });
      return json(res, 400, { error: "Missing email_text" });
    }

    const result = await callOpenAIForClassification(emailText);
    await logEvent("classify_email", { result });
    return json(res, 200, result);
  }

  // Default
  await logEvent("unknown_route", { path: req.url, method: req.method });
  return json(res, 200, { status: "DKT AI server running (secured)" });
});

server.listen(10000, () => console.log("Server running on port 10000"));
