const http = require("http");
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : undefined
});

async function ensureTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_log (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ DEFAULT NOW(),
      event_type TEXT,
      details JSONB
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_config (
      key TEXT PRIMARY KEY,
      value JSONB
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS owner_alerts (
      id BIGSERIAL PRIMARY KEY,
      ts TIMESTAMPTZ DEFAULT NOW(),
      severity TEXT,
      title TEXT,
      message TEXT,
      meta JSONB
    );
  `);
}

async function logEvent(type, details = {}) {
  await pool.query(
    `INSERT INTO audit_log (event_type, details) VALUES ($1, $2)`,
    [type, details]
  );
}

async function setConfig(key, value) {
  await pool.query(
    `INSERT INTO app_config (key, value)
     VALUES ($1, $2)
     ON CONFLICT (key)
     DO UPDATE SET value = EXCLUDED.value`,
    [key, value]
  );
}

async function getConfig(key) {
  const r = await pool.query(`SELECT value FROM app_config WHERE key=$1`, [key]);
  return r.rows[0]?.value;
}

async function createAlert({ severity = "info", title, message, meta = {} }) {
  await pool.query(
    `INSERT INTO owner_alerts (severity, title, message, meta)
     VALUES ($1, $2, $3, $4)`,
    [severity, title, message, meta]
  );
}

async function listAlerts(limit = 25) {
  const r = await pool.query(
    `SELECT * FROM owner_alerts ORDER BY id DESC LIMIT $1`,
    [limit]
  );
  return r.rows;
}

function json(res, status, obj) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve) => {
    let data = "";
    req.on("data", chunk => data += chunk);
    req.on("end", () => resolve(data));
  });
}

async function openaiChatJSON(system, user) {
  const resp = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      model: "gpt-4.1-mini",
      temperature: 0,
      messages: [
        { role: "system", content: system },
        { role: "user", content: user }
      ]
    })
  });

  const data = await resp.json();
  return JSON.parse(data.choices[0].message.content);
}

let ready = false;

const server = http.createServer(async (req, res) => {

  const token = req.headers["authorization"];
  if (token !== `Bearer ${process.env.AUTH_TOKEN}`) {
    return res.writeHead(401).end("Unauthorized");
  }

  if (!ready) {
    await ensureTables();
    ready = true;
  }

  // HEALTH
  if (req.url === "/health") {
    return json(res, 200, { ok: true });
  }

  // CONFIG
  if (req.url === "/config/rules" && req.method === "POST") {
    const body = JSON.parse(await readBody(req));
    await setConfig("rules", body);
    return json(res, 200, { ok: true });
  }

  if (req.url === "/config/rules" && req.method === "GET") {
    const rules = await getConfig("rules");
    return json(res, 200, { rules });
  }

  // ALERTS
  if (req.url === "/alerts" && req.method === "POST") {
    const body = JSON.parse(await readBody(req));
    await createAlert(body);
    return json(res, 200, { ok: true });
  }

  if (req.url.startsWith("/alerts") && req.method === "GET") {
    const alerts = await listAlerts();
    return json(res, 200, { ok: true, alerts });
  }

  // DRAFT REPLY
  if (req.url === "/draft/carrier-reply" && req.method === "POST") {

    const body = JSON.parse(await readBody(req));
    const rules = await getConfig("rules");

    const email = body.email_text || "";
    const ctx = body.load_context || {};

    const system = `
You are DKT Logistics' AI carrier assistant.

FORMAT STRICTLY:

Picks up {pickup_fcfs_or_appt} {pickup_time_window_military} in {pickup_city_state}
Delivers to {delivery_city_state} {delivery_fcfs_or_appt} {delivery_time_window_military}
Truckload of {commodity_desc} weighing {weight_lbs}lbs

Ask for MC ONLY if not found.
Do NOT ask to confirm details if load_context provided.
Return JSON only.
`;

    const user = `
EMAIL:
${email}

LOAD_CONTEXT:
${JSON.stringify(ctx)}
`;

    const result = await openaiChatJSON(system, user);

    if (result.needs_mc_request) {
      await createAlert({
        severity: "info",
        title: "MC missing",
        message: `Carrier asked about ${result.lane_guess}`
      });
    }

    return json(res, 200, { ok: true, result });
  }

  json(res, 200, { status: "running" });
});

server.listen(10000);
