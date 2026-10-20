// Сервер игры «Лес (выживание)»: раздаёт игру и ретранслирует мультиплеер по WebSocket.
// Без зависимостей — нужен только Node.js 16+.
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const PUB = __dirname;
const HIDDEN = new Set(['server.js', 'package.json', 'package-lock.json', 'start.bat', 'start.sh', 'readme.md']); // не отдаём наружу
const MAXMSG = 256 * 1024;
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon', '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.webp': 'image/webp' };

// ---------- HTTP: раздача index.html и файлов рядом с ним ----------
const server = http.createServer((req, res) => {
  let u;
  try { u = decodeURIComponent(req.url.split('?')[0]); } catch (e) { res.writeHead(400); return res.end(); }
  if (u === '/') u = '/index.html';
  const f = path.normalize(path.join(PUB, u));
  if (!f.startsWith(PUB + path.sep) || HIDDEN.has(path.basename(f).toLowerCase()) || path.basename(f).startsWith('.') || f.includes(path.sep + 'node_modules' + path.sep)) { res.writeHead(403); return res.end('Forbidden'); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f).toLowerCase()] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(d);
  });
});

// ---------- WebSocket (минимальная реализация RFC 6455) ----------
const clients = new Map();   // id -> клиент
const rooms = new Map();     // имя комнаты -> Set(id)

function frame(op, p) {
  const l = p.length; let h;
  if (l < 126) h = Buffer.from([128 | op, l]);
  else if (l < 65536) { h = Buffer.alloc(4); h[0] = 128 | op; h[1] = 126; h.writeUInt16BE(l, 2); }
  else { h = Buffer.alloc(10); h[0] = 128 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(l, 6); }
  return Buffer.concat([h, p]);
}
function send(c, o) {
  if (c.sock.destroyed || !c.sock.writable) return;
  try { c.sock.write(frame(1, Buffer.from(JSON.stringify(o)))); } catch (e) {}
}

function parse(c) {
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = !!(b[0] & 128), op = b[0] & 15, masked = !!(b[1] & 128);
    let len = b[1] & 127, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2)) { c.sock.destroy(); return; } len = b.readUInt32BE(6); off = 10; }
    if (len > MAXMSG) { c.sock.destroy(); return; }
    const need = off + (masked ? 4 : 0) + len;
    if (b.length < need) return;
    let payload = b.subarray(off + (masked ? 4 : 0), need);
    if (masked) {
      const m = b.subarray(off, off + 4), out = Buffer.allocUnsafe(len);
      for (let i = 0; i < len; i++) out[i] = payload[i] ^ m[i & 3];
      payload = out;
    }
    c.buf = b.subarray(need);
    if (op === 8) { try { c.sock.end(frame(8, Buffer.alloc(0))); } catch (e) {} return; }
    if (op === 9) { try { c.sock.write(frame(10, payload)); } catch (e) {} continue; }
    if (op === 10) continue;
    if (op === 1 || op === 2) { c.fragOp = op; c.frag = [payload]; }
    else if (op === 0 && c.frag) c.frag.push(payload);
    else continue;
    if (fin) {
      const data = Buffer.concat(c.frag); c.frag = null;
      if (c.fragOp === 1) onMsg(c, data.toString('utf8'));
    }
  }
}

const clean = p => (p && typeof p === 'object' && !Array.isArray(p)) ? p : {};
const members = name => rooms.get(name) || new Set();

function toRoom(c, o) {            // всем в комнате, кроме отправителя
  for (const id of members(c.room)) if (id !== c.id) { const q = clients.get(id); if (q) send(q, o); }
}
function lobbyList() {
  const peers = [];
  for (const q of clients.values()) if (q.lp) peers.push({ peer: q.id, presence: q.lp });
  return peers;
}
function broadcastLobby() {
  const m = { t: 'lobby', peers: lobbyList() };
  for (const q of clients.values()) send(q, m);
}
function leave(c) {
  if (c.room === null) return;
  const name = c.room, set = rooms.get(name);
  c.room = null; c.rp = {};
  if (set) {
    set.delete(c.id);
    if (!set.size) rooms.delete(name);
    else for (const id of set) { const q = clients.get(id); if (q) send(q, { t: 'pl', peer: c.id }); }
  }
}
function onMsg(c, txt) {
  let m; try { m = JSON.parse(txt); } catch (e) { return; }
  if (!m || typeof m.t !== 'string') return;
  switch (m.t) {
    case 'lp':
      c.lp = clean(m.p); broadcastLobby(); break;
    case 'join': {
      const name = String(m.room || '').slice(0, 40);
      if (!name || c.room === name) break;
      leave(c);
      c.room = name; c.rp = {};
      if (!rooms.has(name)) rooms.set(name, new Set());
      rooms.get(name).add(c.id);
      const peers = [...rooms.get(name)].map(id => ({ peer: id, presence: clients.get(id).rp }));
      for (const id of rooms.get(name)) send(clients.get(id), { t: 'rfull', peers });
      break;
    }
    case 'rp':
      if (c.room === null) break;
      c.rp = clean(m.p);
      toRoom(c, { t: 'pu', peer: c.id, presence: c.rp });
      break;
    case 'em':
      if (c.room === null) break;
      toRoom(c, { t: 'msg', type: String(m.type || '').slice(0, 24), data: m.data, from: c.id });
      break;
  }
}
function drop(c) {
  if (!clients.has(c.id)) return;
  leave(c); clients.delete(c.id); broadcastLobby();
}

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { id: crypto.randomBytes(6).toString('hex'), sock, room: null, lp: null, rp: {}, buf: Buffer.alloc(0), frag: null, fragOp: 0, alive: true };
  clients.set(c.id, c);
  sock.on('data', d => { c.alive = true; c.buf = Buffer.concat([c.buf, d]); parse(c); });
  sock.on('close', () => drop(c));
  sock.on('error', () => sock.destroy());
  send(c, { t: 'hello', id: c.id });
  send(c, { t: 'lobby', peers: lobbyList() });
});

// пинг раз в 30 сек: держит соединение за прокси и убирает «мёртвые»
setInterval(() => {
  for (const c of clients.values()) {
    if (!c.alive) { c.sock.destroy(); continue; }
    c.alive = false;
    try { c.sock.write(frame(9, Buffer.alloc(0))); } catch (e) {}
  }
}, 30000).unref();

server.listen(PORT, () => {
  console.log('🌲 Лес (выживание) запущен: http://localhost:' + PORT);
  console.log('Другие игроки в твоей сети: http://<IP-твоего-компьютера>:' + PORT);
});
