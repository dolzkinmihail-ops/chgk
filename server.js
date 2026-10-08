const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

// ----- Статика -----
const server = http.createServer((req, res) => {
  let url = req.url.split('?')[0];
  if (url === '/') url = '/index.html';
  const filePath = path.join(__dirname, 'public', url);
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const ext = path.extname(filePath).toLowerCase();
    const mimes = {
      '.html': 'text/html; charset=utf-8',
      '.js': 'text/javascript; charset=utf-8',
      '.css': 'text/css; charset=utf-8',
      '.json': 'application/json',
      '.png': 'image/png',
      '.svg': 'image/svg+xml',
    };
    res.writeHead(200, { 'Content-Type': mimes[ext] || 'application/octet-stream' });
    res.end(data);
  });
});

// ----- WebSocket -----
const wss = new WebSocket.Server({ server, maxPayload: 50 * 1024 * 1024 });
const rooms = new Map();

const uid = () => crypto.randomBytes(8).toString('hex');
const genCode = () => {
  let code;
  do { code = 'CHGK-' + Math.floor(1000 + Math.random() * 9000); }
  while (rooms.has(code));
  return code;
};

function makeSectors(pkg, tourIndex) {
  const tour = pkg && pkg.tours && pkg.tours[tourIndex];
  const out = [];
  for (let i = 0; i < 12; i++) {
    const src = (tour && tour.questions && tour.questions[i]) || {};
    out.push({
      q: src.q || '',
      qMedia: src.qMedia || src.media || '',
      a: src.a || '',
      aMedia: src.aMedia || '',
      status: 'idle'
    });
  }
  return out;
}

function publicState(room) {
  const active = room.active !== null ? room.sectors[room.active] : null;
  return {
    code: room.code,
    phase: room.phase,
    mode: room.mode,
    roundTime: room.roundTime,
    active: room.active,
    score: room.score,
    revealed: room.revealed,
    timer: { left: room.timer.left, phase: room.timer.phase },
    sectors: room.sectors.map(s => ({ status: s.status })),
    // Медиа вопроса видно ВСЕМ (знатокам тоже)
    activeQMedia: active ? active.qMedia : '',
    // Правильный ответ (текст + медиа) — всем только после reveal
    revealedAnswer: (room.revealed && active) ? { a: active.a, aMedia: active.aMedia } : null,
    players: Array.from(room.players.values()).map(p => ({
      id: p.id, name: p.name, avatar: p.avatar, isHost: p.isHost,
      hasAnswer: !!(p.answer && p.answer.trim())
    }))
  };
}

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function broadcast(room) {
  const state = publicState(room);
  const answers = Array.from(room.players.values()).map(p => ({ id: p.id, name: p.name, answer: p.answer || '', isHost: p.isHost }));
  room.players.forEach(p => {
    if (p.ws.readyState !== WebSocket.OPEN) return;
    send(p.ws, { type: 'state', state, you: p.id, isHost: p.isHost });
    if (p.isHost) {
      send(p.ws, { type: 'answers', answers });
      if (room.active !== null) {
        const s = room.sectors[room.active];
        send(p.ws, {
          type: 'host_question',
          sector: { q: s.q, qMedia: s.qMedia, a: s.a, aMedia: s.aMedia }
        });
      } else {
        send(p.ws, { type: 'host_question', sector: null });
      }
    }
  });
}

function broadcastSpin(room, sectorIndex, duration) {
  room.players.forEach(p => send(p.ws, { type: 'spin', sectorIndex, duration }));
}

function startRoomTimer(room, left) {
  if (room.timer.interval) clearInterval(room.timer.interval);
  room.timer.left = left;
  room.timer.phase = 'running';
  room.timer.endsAt = Date.now() + left * 1000;
  room.timer.interval = setInterval(() => {
    const rem = Math.max(0, Math.ceil((room.timer.endsAt - Date.now()) / 1000));
    room.timer.left = rem;
    if (rem <= 0) {
      clearInterval(room.timer.interval);
      room.timer.interval = null;
      room.timer.phase = 'ended';
    }
    broadcast(room);
  }, 900);
}

function stopRoomTimer(room) {
  if (room.timer.interval) clearInterval(room.timer.interval);
  room.timer.interval = null;
}

wss.on('connection', ws => {
  ws._id = uid();
  ws._roomCode = null;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    try { handle(ws, msg); } catch (e) { console.error(e); }
  });

  ws.on('close', () => {
    const room = rooms.get(ws._roomCode);
    if (!room) return;
    const player = room.players.get(ws._id);
    if (!player) return;
    room.players.delete(ws._id);
    if (room.host === ws) {
      stopRoomTimer(room);
      room.players.forEach(p => send(p.ws, { type: 'error', message: 'Ведущий вышел — комната закрыта' }));
      rooms.delete(room.code);
      return;
    }
    if (room.players.size === 0) {
      stopRoomTimer(room);
      rooms.delete(room.code);
      return;
    }
    broadcast(room);
  });
});

function getRoom(ws) { return rooms.get(ws._roomCode); }
const isHost = (ws, room) => room && room.host === ws;

function handle(ws, msg) {
  switch (msg.type) {
    case 'create': return onCreate(ws, msg);
    case 'join': return onJoin(ws, msg);
    case 'update_player': return onUpdatePlayer(ws, msg);
    case 'start_game': return onStartGame(ws);
    case 'spin': return onSpin(ws);
    case 'open_question': return broadcast(getRoom(ws));
    case 'set_active': return onSetActive(ws, msg);
    case 'next_seq': return onNextSeq(ws);
    case 'award': return onAward(ws, msg);
    case 'minus': return onMinus(ws, msg);
    case 'start_timer': return onStartTimer(ws);
    case 'pause_timer': return onPauseTimer(ws);
    case 'reset_timer': return onResetTimer(ws);
    case 'reset_round': return onResetRound(ws);
    case 'new_game': return onNewGame(ws);
    case 'submit_answer': return onSubmitAnswer(ws, msg);
    case 'reveal': return onReveal(ws);
  }
}

function onCreate(ws, msg) {
  const code = genCode();
  const s = msg.settings || {};
  const room = {
    code, host: ws, players: new Map(), phase: 'lobby',
    mode: s.mode || 'wheel',
    roundTime: s.roundTime || 60,
    sectors: makeSectors(msg.pkg, s.tourIndex || 0),
    active: null,
    score: { experts: 0, host: 0 },
    timer: { left: s.roundTime || 60, phase: 'idle', interval: null, endsAt: null },
    revealed: false
  };
  rooms.set(code, room);
  room.players.set(ws._id, {
    id: ws._id, ws, name: msg.name || 'Ведущий',
    avatar: msg.avatar || null, isHost: true, answer: ''
  });
  ws._roomCode = code;
  send(ws, { type: 'room_created', code, you: ws._id });
  broadcast(room);
}

function onJoin(ws, msg) {
  const room = rooms.get((msg.code || '').toUpperCase());
  if (!room) return send(ws, { type: 'error', message: 'Комната не найдена' });
  if (room.players.size >= 10) return send(ws, { type: 'error', message: 'В комнате уже 10 игроков' });
  if (room.phase !== 'lobby') return send(ws, { type: 'error', message: 'Игра уже началась' });
  room.players.set(ws._id, {
    id: ws._id, ws, name: msg.name || 'Знаток',
    avatar: msg.avatar || null, isHost: false, answer: ''
  });
  ws._roomCode = room.code;
  send(ws, { type: 'room_joined', code: room.code, you: ws._id });
  broadcast(room);
}

function onUpdatePlayer(ws, msg) {
  const room = getRoom(ws); if (!room) return;
  const p = room.players.get(ws._id); if (!p) return;
  if (typeof msg.name === 'string') p.name = msg.name.slice(0, 24);
  if (typeof msg.avatar === 'string' || msg.avatar === null) p.avatar = msg.avatar;
  broadcast(room);
}

function onStartGame(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  room.phase = 'game';
  room.active = null;
  room.timer.phase = 'idle';
  room.timer.left = room.roundTime;
  broadcast(room);
}

function onSpin(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  if (room.timer.phase === 'running') return;
  const free = [];
  room.sectors.forEach((s, i) => { if (s.status === 'idle') free.push(i); });
  if (!free.length) return send(ws, { type: 'error', message: 'Все 12 секторов разыграны' });
  const pick = free[Math.floor(Math.random() * free.length)];
  room.active = null;
  room.revealed = false;
  room.timer.phase = 'idle';
  room.timer.left = room.roundTime;
  room.players.forEach(p => p.answer = '');
  broadcastSpin(room, pick, 5000);
  setTimeout(() => {
    room.active = pick;
    broadcast(room);
  }, 5100);
}

function onSetActive(ws, msg) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  const i = msg.index;
  if (typeof i !== 'number' || i < 0 || i >= 12) return;
  if (room.sectors[i].status !== 'idle') return;
  room.active = i;
  room.revealed = false;
  room.timer.phase = 'idle';
  room.timer.left = room.roundTime;
  room.players.forEach(p => p.answer = '');
  broadcast(room);
}

function onNextSeq(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  const free = [];
  room.sectors.forEach((s, i) => { if (s.status === 'idle') free.push(i); });
  if (!free.length) return send(ws, { type: 'error', message: 'Все 12 секторов разыграны' });
  room.active = free[0];
  room.revealed = false;
  room.timer.phase = 'idle';
  room.timer.left = room.roundTime;
  room.players.forEach(p => p.answer = '');
  broadcast(room);
}

function onAward(ws, msg) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  const i = room.active;
  if (i === null) return send(ws, { type: 'error', message: 'Сектор не выбран' });
  const s = room.sectors[i];
  if (s.status !== 'idle') return;
  s.status = msg.who === 'expert' ? 'expert' : 'host';
  if (msg.who === 'expert') room.score.experts++;
  else room.score.host++;
  room.active = null;
  room.revealed = false;
  room.timer.phase = 'idle';
  stopRoomTimer(room);
  room.players.forEach(p => p.answer = '');
  broadcast(room);
}

function onMinus(ws, msg) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  if (msg.who === 'expert') room.score.experts = Math.max(0, room.score.experts - 1);
  else room.score.host = Math.max(0, room.score.host - 1);
  broadcast(room);
}

function onStartTimer(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  if (room.active === null) return send(ws, { type: 'error', message: 'Сектор не выбран' });
  if (room.timer.phase === 'paused') startRoomTimer(room, room.timer.left);
  else if (room.timer.phase === 'idle') startRoomTimer(room, room.roundTime);
  broadcast(room);
}

function onPauseTimer(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  if (room.timer.phase !== 'running') return;
  stopRoomTimer(room);
  room.timer.phase = 'paused';
  broadcast(room);
}

function onResetTimer(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  stopRoomTimer(room);
  room.timer.phase = 'idle';
  room.timer.left = room.roundTime;
  broadcast(room);
}

function onResetRound(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  room.active = null;
  room.revealed = false;
  stopRoomTimer(room);
  room.timer.phase = 'idle';
  room.timer.left = room.roundTime;
  room.players.forEach(p => p.answer = '');
  broadcast(room);
}

function onNewGame(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  room.sectors.forEach(s => s.status = 'idle');
  room.score.experts = 0;
  room.score.host = 0;
  room.active = null;
  room.revealed = false;
  stopRoomTimer(room);
  room.timer.phase = 'idle';
  room.timer.left = room.roundTime;
  room.players.forEach(p => p.answer = '');
  broadcast(room);
}

function onSubmitAnswer(ws, msg) {
  const room = getRoom(ws); if (!room) return;
  const p = room.players.get(ws._id); if (!p) return;
  p.answer = String(msg.text || '').slice(0, 5000);
  broadcast(room);
}

function onReveal(ws) {
  const room = getRoom(ws);
  if (!isHost(ws, room)) return;
  room.revealed = true;
  broadcast(room);
}

server.listen(PORT, () => {
  console.log(`\n✅ Сервер запущен`);
  console.log(`   Локально:  http://localhost:${PORT}`);
  console.log(`   По сети:   http://<ваш-IP>:${PORT}\n`);
});
