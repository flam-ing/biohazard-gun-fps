const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');
const QRCode = require('qrcode');

const PORT = process.env.PORT || process.env.HTTP_PORT || 3000;
const IS_RENDER = Boolean(process.env.RENDER);
const MAX_PLAYERS = 4;
// 폰이 잠깐 끊겨도(새로고침, 화면 꺼짐) 같은 자리로 돌아올 수 있게 잠시 자리를 잡아 둡니다.
const RESERVE_MS = Number(process.env.RESERVE_MS || 15000);

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

app.get('/healthz', (req, res) => res.json({ ok: true }));

app.get('/api/info', async (req, res) => {
  try {
    const host = req.headers.host;
    const proto = req.headers['x-forwarded-proto'] || (req.secure ? 'https' : 'http');
    const baseUrl = `${proto}://${host}`;
    const controllerHttpsUrl = `${baseUrl}/controller.html`;
    const qrHttpsDataUrl = await QRCode.toDataURL(controllerHttpsUrl, {
      margin: 1,
      width: 320,
      color: { dark: '#000000', light: '#ffffff' }
    });
    res.json({ baseUrl, controllerHttpsUrl, qrHttpsDataUrl, isRender: IS_RENDER });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  pingInterval: 10000,
  pingTimeout: 8000
});

const PLAYER_SLOTS = {
  1: { slot: 1, name: '1P', color: '#ff4d5a' },
  2: { slot: 2, name: '2P', color: '#2ec5ff' },
  3: { slot: 3, name: '3P', color: '#ffd23f' },
  4: { slot: 4, name: '4P', color: '#6bf178' }
};

// slot 번호 -> { slot, token, socketId, connected, timer }
const slots = new Map();
// 마지막으로 화면(PC)이 알려 준 게임 상태. 새로 들어온 폰에게 바로 알려 줍니다.
let lastGameState = { phase: 'lobby' };

// 폰 -> 화면으로만 보내는 이벤트
const CONTROLLER_EVENTS = [
  'aim',
  'shoot',
  'reload',
  'center_reset',
  'calib_begin',
  'settings',
  'weapon_change',
  'start_game'
];
// 화면 -> 특정 폰(또는 모든 폰)으로 보내는 이벤트
const DISPLAY_EVENTS = ['ammo_update', 'game_state', 'hit', 'need_center'];

function publicPlayer(entry) {
  return { ...PLAYER_SLOTS[entry.slot], connected: entry.connected };
}

function playerList() {
  return [...slots.values()].sort((a, b) => a.slot - b.slot).map(publicPlayer);
}

function displayCount() {
  const room = io.sockets.adapter.rooms.get('displays');
  return room ? room.size : 0;
}

function broadcastPlayers() {
  const payload = { players: playerList(), count: slots.size, max: MAX_PLAYERS };
  io.to('displays').emit('player_list_updated', payload);
  io.to('controllers').emit('player_list_updated', payload);
}

function releaseSlot(slot) {
  const entry = slots.get(slot);
  if (!entry) return;
  clearTimeout(entry.timer);
  slots.delete(slot);
}

function cleanToken(token) {
  if (typeof token !== 'string') return null;
  const t = token.trim().slice(0, 64);
  return t.length >= 6 ? t : null;
}

function registerController(socket, token) {
  // 같은 소켓이 다시 등록하면 이미 받은 자리를 그대로 돌려줍니다.
  if (socket.data.slot && slots.get(socket.data.slot)?.socketId === socket.id) {
    const entry = slots.get(socket.data.slot);
    socket.emit('player_assigned', { ...publicPlayer(entry), rejoined: true });
    return;
  }

  let entry = token ? [...slots.values()].find((e) => e.token === token) : null;
  let rejoined = false;

  if (entry) {
    // 같은 폰이 다시 들어온 경우: 이전 소켓이 아직 살아 있으면 자리를 넘겨받습니다.
    rejoined = true;
    clearTimeout(entry.timer);
    if (entry.socketId && entry.socketId !== socket.id) {
      const old = io.sockets.sockets.get(entry.socketId);
      if (old) {
        old.leave(`slot:${entry.slot}`);
        old.leave('controllers');
        old.data.slot = null;
        old.emit('replaced');
      }
    }
  } else {
    let free = null;
    for (let s = 1; s <= MAX_PLAYERS; s++) {
      if (!slots.has(s)) {
        free = s;
        break;
      }
    }
    if (!free) {
      socket.emit('room_full', { max: MAX_PLAYERS, message: '4명이 꽉 찼어요' });
      console.log(`[full] 5번째 컨트롤러 거절: ${socket.id}`);
      return;
    }
    entry = { slot: free, token: token || `anon-${socket.id}` };
    slots.set(free, entry);
  }

  entry.socketId = socket.id;
  entry.connected = true;
  entry.timer = null;
  socket.data.slot = entry.slot;
  socket.join('controllers');
  socket.join(`slot:${entry.slot}`);

  socket.emit('player_assigned', { ...publicPlayer(entry), rejoined });
  socket.emit('display_status', { connected: displayCount() > 0 });
  socket.emit('game_state', lastGameState);
  broadcastPlayers();
  console.log(`[controller] ${entry.slot}P ${rejoined ? '재접속' : '연결'}: ${socket.id}`);
}

io.on('connection', (socket) => {
  socket.on('register_role', (payload) => {
    const role = typeof payload === 'string' ? payload : payload && payload.role;
    const token = cleanToken(payload && payload.token);

    if (role === 'controller') {
      socket.data.role = 'controller';
      registerController(socket, token);
    } else if (role === 'display') {
      socket.data.role = 'display';
      socket.join('displays');
      socket.emit('player_list_updated', { players: playerList(), count: slots.size, max: MAX_PLAYERS });
      io.to('controllers').emit('display_status', { connected: true });
      // 화면이 새로 켜졌으니 폰들에게 설정(감도, 무기, 영점 상태)을 다시 보내 달라고 합니다.
      io.to('controllers').emit('display_joined');
      console.log(`[display] 연결: ${socket.id}`);
    }
  });

  // 폰 -> 화면: 보낸 폰의 자리 정보를 붙여서 화면에만 전달합니다. 다른 폰에는 가지 않습니다.
  CONTROLLER_EVENTS.forEach((eventName) => {
    socket.on(eventName, (data) => {
      if (socket.data.role !== 'controller') return;
      const slot = socket.data.slot;
      const entry = slot && slots.get(slot);
      if (!entry || entry.socketId !== socket.id) return;
      const payload = data && typeof data === 'object' && !Array.isArray(data) ? { ...data } : {};
      payload.slot = slot;
      payload.color = PLAYER_SLOTS[slot].color;
      payload.playerName = PLAYER_SLOTS[slot].name;
      if (eventName === 'aim') {
        io.to('displays').volatile.emit('aim', payload);
      } else {
        io.to('displays').emit(eventName, payload);
      }
    });
  });

  // 화면 -> 특정 자리의 폰
  socket.on('to_slot', (msg) => {
    if (socket.data.role !== 'display' || !msg) return;
    const slot = Number(msg.slot);
    if (!PLAYER_SLOTS[slot] || !DISPLAY_EVENTS.includes(msg.event)) return;
    io.to(`slot:${slot}`).emit(msg.event, msg.data || {});
  });

  // 화면 -> 모든 폰
  socket.on('to_controllers', (msg) => {
    if (socket.data.role !== 'display' || !msg || !DISPLAY_EVENTS.includes(msg.event)) return;
    if (msg.event === 'game_state' && msg.data && typeof msg.data.phase === 'string') {
      lastGameState = { phase: msg.data.phase };
    }
    io.to('controllers').emit(msg.event, msg.data || {});
  });

  socket.on('disconnect', () => {
    if (socket.data.role === 'display') {
      if (displayCount() === 0) {
        lastGameState = { phase: 'lobby' };
        io.to('controllers').emit('display_status', { connected: false });
      }
      console.log(`[display] 연결 끊김: ${socket.id}`);
      return;
    }
    const slot = socket.data.slot;
    const entry = slot && slots.get(slot);
    if (!entry || entry.socketId !== socket.id) return;
    entry.connected = false;
    entry.socketId = null;
    entry.timer = setTimeout(() => {
      releaseSlot(slot);
      broadcastPlayers();
      console.log(`[controller] ${slot}P 자리 비움`);
    }, RESERVE_MS);
    broadcastPlayers();
    console.log(`[controller] ${slot}P 연결 끊김 (${RESERVE_MS / 1000}초 동안 자리 유지)`);
  });
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`BIOHAZARD SMART-GUN 서버 시작: http://localhost:${httpServer.address().port}`);
});

module.exports = { httpServer, io };
