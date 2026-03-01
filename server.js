const http = require("http");

function unauthorized(res) {
  res.writeHead(401, { "Content-Type": "text/plain" });
  res.end("Unauthorized");
}

const server = http.createServer((req, res) => {
  // Require a secret token to access the service
  const token = req.headers["authorization"];
  const expected = process.env.AUTH_TOKEN;

  if (!expected) {
    res.writeHead(500, { "Content-Type": "text/plain" });
    return res.end("Server misconfigured: missing AUTH_TOKEN");
  }

  if (!token || token !== `Bearer ${expected}`) {
    return unauthorized(res);
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("DKT AI server is running (secured)");
});

server.listen(10000, () => {
  console.log("Server running on port 10000");
});
