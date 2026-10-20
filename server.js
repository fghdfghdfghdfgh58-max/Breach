const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { promisify } = require('util'); const scrypt = promisify(crypto.scrypt);
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const DIR = process.env.DATA_DIR || path.join(__dirname, 'data'), FILE = path.join(DIR, 'data.json');
fs.mkdirSync(DIR, { recursive: true });
let db = { users: {}, sessions: {} };
try { db = JSON.parse(fs.readFileSync(FILE, 'utf8')); } catch {}
db.users = db.users || {}; db.sessions = db.sessions || {};
for (const [t, s] of Object.entries(db.sessions)) if (s.exp < Date.now()) delete db.sessions[t];
let dirty = false; const markDirty = () => { dirty = true; };
setInterval(() => { if (!dirty) return; dirty = false; const t = FILE + '.tmp'; try { fs.writeFileSync(t, JSON.stringify(db)); fs.renameSync(t, FILE); } catch (e) { console.error(e); } }, 3000);
const profile = u => ({ name: u.name, kills: u.kills, deaths: u.deaths, games: u.games });
const json = (res, code, o) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(o)); };
const body = req => new Promise(r => { let s = ''; req.on('data', c => { s += c; if (s.length > 4096) req.destroy(); }); req.on('end', () => { try { r(JSON.parse(s || '{}')); } catch { r({}); } }); });
const sessionOf = token => { const s = db.sessions[token]; if (!s || s.exp < Date.now()) return null; const u = db.users[s.u]; return u ? { key: s.u, u } : null; };
const tokenOf = req => (req.headers.authorization || '').replace(/^Bearer /, '');
const issue = key => { const t = crypto.randomBytes(24).toString('hex'); db.sessions[t] = { u: key, exp: Date.now() + 30 * 864e5 }; markDirty(); return t; };
const fails = new Map();
const ipOf = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
const blocked = ip => { const f = fails.get(ip); return f && Date.now() - f.t < 6e5 && f.n >= 10; };
const fail = ip => { const f = fails.get(ip); fails.set(ip, f && Date.now() - f.t < 6e5 ? { n: f.n + 1, t: f.t } : { n: 1, t: Date.now() }); };
const rooms = new Map(), active = new Set();
const AL = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789';
const page = path.join(__dirname, 'public', 'index.html');

const server = http.createServer(async (req, res) => {
  const url = req.url.split('?')[0];
  try {
    if (url === '/healthz') return res.end('ok');
    if (url === '/api/rooms') {
      const list = [...rooms.values()].filter(r => r.pub && r.peers.size < 16).map(r => ({ code: r.code, name: r.name, players: r.peers.size })).sort((a, b) => b.players - a.players);
      return json(res, 200, { rooms: list });
    }
    if (url === '/api/me') { const s = sessionOf(tokenOf(req)); return s ? json(res, 200, { user: profile(s.u) }) : json(res, 401, { error: 'Not logged in' }); }
    if (url === '/api/logout' && req.method === 'POST') { delete db.sessions[tokenOf(req)]; markDirty(); return json(res, 200, {}); }
    if ((url === '/api/register' || url === '/api/login') && req.method === 'POST') {
      const ip = ipOf(req); if (blocked(ip)) return json(res, 429, { error: 'Too many attempts. Try again in a few minutes.' });
      const b = await body(req), name = String(b.username || '').trim(), pw = String(b.password || ''), key = name.toLowerCase();
      if (url === '/api/register') {
        if (!/^[A-Za-z0-9_]{3,16}$/.test(name)) return json(res, 400, { error: 'Username: 3-16 letters, numbers or underscores.' });
        if (pw.length < 6 || pw.length > 72) return json(res, 400, { error: 'Password must be 6-72 characters.' });
        if (db.users[key]) return json(res, 409, { error: 'That username is taken.' });
        const salt = crypto.randomBytes(16).toString('hex'), hash = (await scrypt(pw, salt, 64)).toString('hex');
        db.users[key] = { name, salt, hash, kills: 0, deaths: 0, games: 0, created: Date.now() };
        return json(res, 200, { token: issue(key), user: profile(db.users[key]) });
      }
      const u = db.users[key];
      const ok = u && crypto.timingSafeEqual(Buffer.from(u.hash, 'hex'), await scrypt(pw, u.salt, 64));
      if (!ok) { fail(ip); return json(res, 401, { error: 'Wrong username or password.' }); }
      return json(res, 200, { token: issue(key), user: profile(u) });
    }
    fs.readFile(page, (e, d) => { if (e) { res.writeHead(500); return res.end('error'); } res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(d); });
  } catch (e) { console.error(e); json(res, 500, { error: 'Server error' }); }
});

const wss = new WebSocketServer({ server, maxPayload: 8192 });
const KEYS = ['n', 'x', 'y', 'z', 'ry', 'hp', 'k', 'd', 'lk', 'c', 'sh', 'so', 'sd', 'mv'];
let counter = 0;
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
function clean(p) {
  const o = {}; if (!p || typeof p !== 'object') return o;
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
  let room = null, me = null, key = null;
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join' && !room) {
      const s = sessionOf(String(m.token || '')); if (!s) return send(ws, { t: 'err', msg: 'Please log in again.' });
      if (active.has(s.key)) return send(ws, { t: 'err', msg: 'This account is already in a game.' });
      let r;
      if (m.create) {
        let code; do { code = Array.from({ length: 5 }, () => AL[Math.random() * AL.length | 0]).join(''); } while (rooms.has(code));
        const nm = String(m.rname || '').replace(/[^\w \-'!.]/g, '').trim().slice(0, 24) || s.u.name + "'s room";
        r = { code, name: nm, pub: !!m.pub, peers: new Map() }; rooms.set(code, r);
      } else {
        r = rooms.get(String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 6));
        if (!r) return send(ws, { t: 'err', msg: 'Room not found. It may have closed.' });
      }
      if (r.peers.size >= 16) return send(ws, { t: 'err', msg: 'Room is full (16 players).' });
      room = r; key = s.key; active.add(key); s.u.games++; markDirty();
      const others = {}; for (const [pid, p] of r.peers) others[pid] = p.pr;
      me = { ws, pr: { n: s.u.name }, d: 0, lastD: 0, key }; r.peers.set(id, me);
      send(ws, { t: 'hello', id, code: r.code, rname: r.name, pub: r.pub, peers: others });
    } else if (m.t === 'p' && room && me) {
      const pr = clean(m.pr), u = db.users[key]; pr.n = u.name;
      if (typeof pr.d === 'number' && pr.d === me.d + 1 && Date.now() - me.lastD > 2500) {
        me.d = pr.d; me.lastD = Date.now(); u.deaths++;
        const k = room.peers.get(pr.lk); if (k && k !== me && db.users[k.key]) db.users[k.key].kills++;
        markDirty();
      }
      Object.assign(me.pr, pr);
      for (const [pid, p] of room.peers) if (pid !== id) send(p.ws, { t: 'p', id, pr });
    }
  });
  ws.on('close', () => {
    if (key) active.delete(key);
    if (!room) return;
    room.peers.delete(id);
    for (const p of room.peers.values()) send(p.ws, { t: 'l', id });
    if (room.peers.size === 0) rooms.delete(room.code);
  });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; ws.ping(); } }, 25000);
server.listen(PORT, () => console.log('Arena FPS running on port ' + PORT));
