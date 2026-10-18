const express = require('express');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { Server } = require('socket.io');
const QRCode = require('qrcode');

const PORT = process.env.PORT || process.env.HTTP_PORT || 3000;
const IS_RENDER = Boolean(process.env.RENDER || process.env.PORT);

const app = express();
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/info', async (req, res) => {
  try {
    const host = req.headers.host;
    const proto = req.headers['x-forwarded-proto'] || (req.secure ? 'https' : 'http');
    const baseUrl = `${proto}://${host}`;
    const controllerHttpsUrl = `${baseUrl}/controller.html`;

    const qrHttpsDataUrl = await QRCode.toDataURL(controllerHttpsUrl, {
      margin: 1,
      width: 280,
      color: { dark: '#000000', light: '#ffffff' }
    });

    res.json({
      baseUrl,
      controllerHttpsUrl,
      qrHttpsDataUrl,
      isRender: IS_RENDER
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const httpServer = http.createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

const PLAYER_SLOTS = {
  1: { slot: 1, name: 'P1', color: '#ef4444', label: '🔴 1P (RED)' },
  2: { slot: 2, name: 'P2', color: '#06b6d4', label: '🔵 2P (BLUE)' },
  3: { slot: 3, name: 'P3', color: '#eab308', label: '🟡 3P (YELLOW)' },
  4: { slot: 4, name: 'P4', color: '#22c55e', label: '🟢 4P (GREEN)' }
};

// socket.id -> playerInfo
const controllerPlayers = new Map();

function assignPlayerSlot(socketId) {
  const usedSlots = new Set([...controllerPlayers.values()].map(p => p.slot));
  for (let s = 1; s <= 4; s++) {
    if (!usedSlots.has(s)) {
      const pInfo = { ...PLAYER_SLOTS[s], id: socketId };
      controllerPlayers.set(socketId, pInfo);
      return pInfo;
    }
  }
  const overflow = { slot: 4, name: 'P4', color: '#22c55e', label: '🟢 4P (GREEN)', id: socketId };
  controllerPlayers.set(socketId, overflow);
  return overflow;
}

function getActivePlayers() {
  return [...controllerPlayers.values()];
}

io.on('connection', (socket) => {
  socket.on('register_role', (role) => {
    socket.data.role = role;
    if (role === 'controller') {
      const pInfo = assignPlayerSlot(socket.id);
      socket.emit('player_assigned', pInfo);
      io.emit('player_list_updated', {
        players: getActivePlayers(),
        count: controllerPlayers.size
      });
      console.log(`📱 [${pInfo.label} 건 컨트롤러 연결됨] ID: ${socket.id}`);
    } else if (role === 'display') {
      socket.emit('player_list_updated', {
        players: getActivePlayers(),
        count: controllerPlayers.size
      });
      console.log(`🖥️ [PC 게임 화면 연결됨] ID: ${socket.id}`);
    }
  });

  function forwardWithSlot(eventName) {
    socket.on(eventName, (data = {}) => {
      const p = controllerPlayers.get(socket.id);
      if (p) {
        data.slot = p.slot;
        data.color = p.color;
        data.playerName = p.name;
      }
      socket.broadcast.emit(eventName, data);
    });
  }

  forwardWithSlot('aim');
  forwardWithSlot('shoot');
  forwardWithSlot('reload');
  forwardWithSlot('calibrate');
  forwardWithSlot('weapon_change');
  forwardWithSlot('game_action');
  forwardWithSlot('sync_state');
  forwardWithSlot('tuning_mode_start');
  forwardWithSlot('confirm_start_game');
  forwardWithSlot('request_recalib');
  forwardWithSlot('adjust_sens');
  forwardWithSlot('change_preset');
  forwardWithSlot('request_center_reset');
  forwardWithSlot('toggle_test_targets');
  forwardWithSlot('set_angle_span');
  forwardWithSlot('set_center_zero');
  forwardWithSlot('ammo_update');

  socket.on('game_started', (data) => socket.broadcast.emit('game_started', data));

  socket.on('disconnect', () => {
    if (controllerPlayers.has(socket.id)) {
      const p = controllerPlayers.get(socket.id);
      controllerPlayers.delete(socket.id);
      io.emit('player_list_updated', {
        players: getActivePlayers(),
        count: controllerPlayers.size
      });
      console.log(`📱 [${p.label} 컨트롤러 연결 해제] ID: ${socket.id}`);
    }
  });
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n========================================================`);
  console.log(`🧟 BIOHAZARD FPS 프로덕션 서버 가동 완료! (포트: ${PORT})`);
  console.log(`========================================================\n`);
});
