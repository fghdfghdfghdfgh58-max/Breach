const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const page = path.join(__dirname, 'public', 'index.html');
const server = http.createServer((req, res) => {
  if (req.url === '/healthz') return res.end('ok');
  fs.readFile(page, (e, d) => {
    if (e) { res.writeHead(500); return res.end('error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(d);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 8192 });
const rooms = new Map(); // code -> Map(id -> {ws, pr})
const KEYS = ['n','x','y','z','ry','hp','k','d','lk','c','sh','so','sd','mv'];
let counter = 0;
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
function clean(p) {
  const o = {};
  if (!p || typeof p !== 'object') return o;
  for (const k of KEYS) {
    const v = p[k];
    if (typeof v === 'number' && Number.isFinite(v)) o[k] = v;
    else if (typeof v === 'string') o[k] = v.slice(0, 24);
    else if (Array.isArray(v) && v.length === 3 && v.every(n => typeof n === 'number' && Number.isFinite(n))) o[k] = v;
  }
  return o;
}
wss.on('connection', ws => {
  const id = 'p' + (++counter).toString(36) + Math.random().toString(36).slice(2, 6);
  let code = null, peers = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join' && !peers) {
      const c = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6);
      if (c.length < 4) return send(ws, { t: 'err', msg: 'Bad room code.' });
      let r = rooms.get(c);
      if (!r) {
        if (!m.create) return send(ws, { t: 'err', msg: 'Room not found. Check the code.' });
        r = new Map(); rooms.set(c, r);
      }
      if (r.size >= 16) return send(ws, { t: 'err', msg: 'Room is full (16 players).' });
      code = c; peers = r;
      const others = {};
      for (const [pid, p] of r) others[pid] = p.pr;
      r.set(id, { ws, pr: {} });
      send(ws, { t: 'hello', id, peers: others });
    } else if (m.t === 'p' && peers) {
      const me = peers.get(id); if (!me) return;
      const pr = clean(m.pr);
      Object.assign(me.pr, pr);
      for (const [pid, p] of peers) if (pid !== id) send(p.ws, { t: 'p', id, pr });
    }
  });
  ws.on('close', () => {
    if (!peers) return;
    peers.delete(id);
    for (const p of peers.values()) send(p.ws, { t: 'l', id });
    if (peers.size === 0) rooms.delete(code);
  });
});
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 25000);
server.listen(PORT, () => console.log('Arena FPS running on port ' + PORT));
