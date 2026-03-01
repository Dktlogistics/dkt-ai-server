const http = require("http");
const { Pool } = require("pg");

// --- DB (Render provides DATABASE_URL) ---
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("render.com")
    ? { rejectUnauthorized: false }
    : undefined
});

async function ensureTables() {
  // Audit log table (minimal)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      event_type TEXT NOT NULL,
      details JSONB NOT NULL DEFAULT '{}'::jsonb
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
    // Don’t crash the server if DB logging fails
    console.error("Audit log insert failed:", e?.message || e);
  }
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
      { role: "user", content: `Classify this email:\n\n${text}` }
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

  const data = await resp.json();
  const content = data.choices?.[0]?.message?.content || "{}";

  try {
    return JSON.parse(content);
  } catch {
    return { category: "unknown", confidence: 0, reason: "Model returned non-JSON" };
  }
}

let tablesReady = false;

const server = http.createServer(async (req, res) => {
  const token = req.headers["authorization"];
  const expected = process.env.AUTH_TOKEN;

  if (!expected) return json(res, 500, { error: "Server misconfigured: missing AUTH_TOKEN" });
  if (!token || token !== `Bearer ${expected}`) return unauthorized(res);

  // Ensure DB tables once per process
  if (!tablesReady) {
    try {
      await ensureTables();
      tablesReady = true;
      await logEvent("server_start", { ok: true });
    } catch (e) {
      return json(res, 500, { error: "DB init failed", details: e?.message || String(e) });
    }
  }

  // Health check
  if (req.url === "/health") {
    await logEvent("health_check", {});
    return json(res, 200, { ok: true });
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

  await logEvent("unknown_route", { path: req.url, method: req.method });
  return json(res, 200, { status: "DKT AI server running (secured)" });
});

server.listen(10000, () => console.log("Server running on port 10000"));
