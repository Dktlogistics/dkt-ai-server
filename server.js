const http = require("http");

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("DKT AI server is running");
});

server.listen(10000, () => {
  console.log("Server running on port 10000");
});
