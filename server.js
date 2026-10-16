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

let connectedControllers = new Set();

io.on('connection', (socket) => {
  socket.on('register_role', (role) => {
    socket.data.role = role;
    if (role === 'controller') {
      connectedControllers.add(socket.id);
      io.emit('controller_status', {
        connected: connectedControllers.size > 0,
        count: connectedControllers.size
      });
      console.log(`📱 [아이폰 건 컨트롤러 연결됨] ID: ${socket.id}`);
    } else if (role === 'display') {
      socket.emit('controller_status', {
        connected: connectedControllers.size > 0,
        count: connectedControllers.size
      });
      console.log(`🖥️ [PC 게임 화면 연결됨] ID: ${socket.id}`);
    }
  });

  socket.on('aim', (data) => socket.broadcast.emit('aim', data));
  socket.on('shoot', (data) => socket.broadcast.emit('shoot', data));
  socket.on('reload', (data) => socket.broadcast.emit('reload', data));
  socket.on('calibrate', (data) => socket.broadcast.emit('calibrate', data));
  socket.on('weapon_change', (data) => socket.broadcast.emit('weapon_change', data));
  socket.on('game_action', (data) => socket.broadcast.emit('game_action', data));
  socket.on('sync_state', (data) => socket.broadcast.emit('sync_state', data));
  socket.on('tuning_mode_start', (data) => socket.broadcast.emit('tuning_mode_start', data));
  socket.on('game_started', (data) => socket.broadcast.emit('game_started', data));
  socket.on('confirm_start_game', (data) => socket.broadcast.emit('confirm_start_game', data));
  socket.on('request_recalib', (data) => socket.broadcast.emit('request_recalib', data));
  socket.on('adjust_sens', (data) => socket.broadcast.emit('adjust_sens', data));
  socket.on('change_preset', (data) => socket.broadcast.emit('change_preset', data));
  socket.on('request_center_reset', (data) => socket.broadcast.emit('request_center_reset', data));

  socket.on('disconnect', () => {
    if (connectedControllers.has(socket.id)) {
      connectedControllers.delete(socket.id);
      io.emit('controller_status', {
        connected: connectedControllers.size > 0,
        count: connectedControllers.size
      });
      console.log(`📱 [컨트롤러 연결 해제] ID: ${socket.id}`);
    }
  });
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`\n========================================================`);
  console.log(`🧟 BIOHAZARD FPS 프로덕션 서버 가동 완료! (포트: ${PORT})`);
  console.log(`========================================================\n`);
});
