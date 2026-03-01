const http = require("http");

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

  const data = await resp.json();
  const content = data.choices?.[0]?.message?.content || "{}";

  try {
    return JSON.parse(content);
  } catch {
    return { category: "unknown", confidence: 0 };
  }
}

const server = http.createServer(async (req, res) => {
  const token = req.headers["authorization"];
  const expected = process.env.AUTH_TOKEN;

  if (!token || token !== `Bearer ${expected}`) {
    return unauthorized(res);
  }

  if (req.url === "/classify-email" && req.method === "POST") {
    const bodyText = await readBody(req);
    const body = JSON.parse(bodyText || "{}");

    const result = await callOpenAIForClassification(body.email_text || "");

    return json(res, 200, result);
  }

  return json(res, 200, { status: "DKT AI server running" });
});

server.listen(10000);
