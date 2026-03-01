const http = require("http");

function unauthorized(res) {
  res.writeHead(401, { "Content-Type": "text/plain" });
  res.end("Unauthorized");
}

function ok(res, text) {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end(text);
}

const server = http.createServer((req, res) => {
  const token = req.headers["authorization"];
  const expected = process.env.AUTH_TOKEN;

  if (!expected) {
    res.writeHead(500, { "Content-Type": "text/plain" });
    return res.end("Server misconfigured");
  }

  // Allow an easy health check endpoint (still secured)
  if (req.url === "/health") {
    if (!token || token !== `Bearer ${expected}`) return unauthorized(res);
    return ok(res, "OK");
  }

  // Default route (still secured)
  if (!token || token !== `Bearer ${expected}`) return unauthorized(res);
  return ok(res, "DKT AI server is running (secured)");
});

server.listen(10000, () => {
  console.log("Server running on port 10000");
});
