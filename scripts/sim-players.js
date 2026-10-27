#!/usr/bin/env node
/*
 * 폰 없이 4인 협동을 확인하는 시뮬레이터입니다.
 *
 *   npm test                         서버 라우팅 검사 + (Chrome이 있으면) 화면 로직 검사
 *   node scripts/sim-players.js --screens screenshots   스크린샷도 저장
 *   node scripts/sim-players.js --no-browser            서버 검사만
 *
 * Chrome 위치는 CHROME_PATH 환경변수로 바꿀 수 있어요.
 */
const path = require('path');
const fs = require('fs');
const { fork } = require('child_process');
const { io } = require('socket.io-client');

const args = process.argv.slice(2);
const NO_BROWSER = args.includes('--no-browser');
const screensIdx = args.indexOf('--screens');
const SCREENS_DIR = screensIdx >= 0 ? path.resolve(args[screensIdx + 1] || 'screenshots') : null;
const RESERVE_MS = 700;
// 브라우저 검사에서는 폰 페이지를 새로고침하고 "시작하기"를 다시 누를 시간이 필요해서 더 길게 잡습니다.
const BROWSER_RESERVE_MS = 3000;
const CONTROLLER_EVENTS = ['aim', 'shoot', 'reload', 'center_reset', 'calib_begin', 'settings', 'weapon_change', 'start_game'];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let failures = 0;
let passes = 0;
function check(cond, label, extra) {
  if (cond) {
    passes++;
    console.log(`  ok   ${label}`);
  } else {
    failures++;
    console.log(`  FAIL ${label}${extra !== undefined ? `  (${JSON.stringify(extra)})` : ''}`);
  }
}
const near = (a, b, eps = 0.05) => Math.abs(a - b) <= eps;

function startServer(reserveMs = RESERVE_MS) {
  return new Promise((resolve, reject) => {
    const port = 4100 + Math.floor(Math.random() * 800);
    const child = fork(path.join(__dirname, '..', 'server.js'), [], {
      env: { ...process.env, PORT: String(port), RESERVE_MS: String(reserveMs), RENDER: '' },
      stdio: ['ignore', 'pipe', 'pipe', 'ipc']
    });
    let done = false;
    child.stdout.on('data', (buf) => {
      if (!done && /서버 시작/.test(String(buf))) {
        done = true;
        resolve({ child, url: `http://localhost:${port}` });
      }
    });
    child.stderr.on('data', (buf) => process.stderr.write(buf));
    child.on('exit', (code) => { if (!done) reject(new Error(`server exited ${code}`)); });
    setTimeout(() => { if (!done) reject(new Error('server start timeout')); }, 8000);
  });
}

// 이벤트를 모두 기록하는 소켓
function client(url, label) {
  const s = io(url, { transports: ['websocket'], forceNew: true, reconnection: false });
  s.label = label;
  s.log = [];
  s.onAny((event, data) => s.log.push({ event, data }));
  return s;
}
function waitFor(s, event, ms = 2000, pred = () => true) {
  return new Promise((resolve, reject) => {
    const found = s.log.find((l) => l.event === event && pred(l.data));
    if (found) return resolve(found.data === undefined ? true : found.data);
    const t = setTimeout(() => {
      s.offAny(h);
      reject(new Error(`${s.label}: ${event} 안 옴`));
    }, ms);
    function h(ev, data) {
      if (ev === event && pred(data)) {
        clearTimeout(t);
        s.offAny(h);
        resolve(data === undefined ? true : data);
      }
    }
    s.onAny(h);
  });
}
const received = (s, event) => s.log.filter((l) => l.event === event);
async function connected(s) {
  if (s.connected) return;
  await new Promise((r) => s.once('connect', r));
}
async function controller(url, label, token) {
  const s = client(url, label);
  s.token = token;
  await connected(s);
  s.emit('register_role', { role: 'controller', token });
  return s;
}
async function display(url, label = 'display') {
  const s = client(url, label);
  await connected(s);
  s.emit('register_role', 'display');
  await waitFor(s, 'player_list_updated');
  return s;
}

// ---------------------------------------------------------------------------
// 1. 서버 라우팅 검사
// ---------------------------------------------------------------------------
async function serverTests() {
  console.log('\n[서버] 자리 배정과 이벤트 전달');
  const { child, url } = await startServer();
  const sockets = [];
  try {
    const D = await display(url);
    sockets.push(D);
    const C = [];
    for (let i = 1; i <= 4; i++) {
      const c = await controller(url, `C${i}`, `token-player-${i}`);
      sockets.push(c);
      const a = await waitFor(c, 'player_assigned');
      c.slot = a.slot;
      C.push(c);
    }
    check(C.map((c) => c.slot).join(',') === '1,2,3,4', '4명이 1P~4P에 하나씩 배정됨', C.map((c) => c.slot));
    const list = await waitFor(D, 'player_list_updated', 2000, (d) => d.count === 4);
    check(list.players.length === 4, '화면이 4명 목록을 받음');

    const C5 = await controller(url, 'C5', 'token-player-5');
    sockets.push(C5);
    const full = await waitFor(C5, 'room_full').catch(() => null);
    await sleep(150);
    check(!!full && received(C5, 'player_assigned').length === 0, '5번째 폰은 "4명이 꽉 찼어요"로 거절됨', full);

    // 이벤트 전달
    C.forEach((c) => { c.log = []; });
    D.log = [];
    C[0].emit('aim', { yaw: 5, pitch: 1, epoch: 0 });
    C[1].emit('shoot', { yaw: 0, pitch: 0 });
    C[2].emit('reload', { method: 'swipe' });
    C[1].emit('center_reset', { epoch: 1 });
    C[0].emit('aim', { yaw: 1, pitch: 1, slot: 3 }); // 다른 자리인 척
    await sleep(300);
    const aims = received(D, 'aim');
    check(aims.length === 2 && aims.every((a) => a.data.slot === 1), '화면이 1P aim을 받고, 자리를 속여도 1P로 붙음', aims.map((a) => a.data.slot));
    check(received(D, 'shoot')[0]?.data.slot === 2, '2P shoot이 slot 2로 전달됨');
    check(received(D, 'reload')[0]?.data.slot === 3, '3P reload가 slot 3으로 전달됨');
    const cr = received(D, 'center_reset');
    check(cr.length === 1 && cr[0].data.slot === 2, '2P 가운데 맞추기는 slot 2만 표시됨');
    const leaked = C.flatMap((c) => c.log.filter((l) => CONTROLLER_EVENTS.includes(l.event)).map((l) => `${c.label}:${l.event}`));
    check(leaked.length === 0, '폰은 다른 폰의 이벤트를 받지 않음', leaked);

    // 화면 -> 특정 폰
    C.forEach((c) => { c.log = []; });
    D.emit('to_slot', { slot: 2, event: 'ammo_update', data: { ammo: 7, maxAmmo: 12 } });
    D.emit('to_controllers', { event: 'game_state', data: { phase: 'playing' } });
    C[0].emit('to_slot', { slot: 2, event: 'ammo_update', data: { ammo: 0 } }); // 폰은 보낼 수 없음
    await sleep(300);
    const ammoGot = C.map((c) => received(c, 'ammo_update').map((l) => l.data.ammo));
    check(JSON.stringify(ammoGot) === JSON.stringify([[], [7], [], []]), '탄 정보는 2P 폰에만 감 (폰끼리는 못 보냄)', ammoGot);
    check(C.every((c) => received(c, 'game_state').length === 1), '게임 상태는 모든 폰에 감');

    // 재접속: 같은 토큰이면 같은 자리
    D.log = [];
    C[2].disconnect();
    const away = await waitFor(D, 'player_list_updated', 2000, (d) => d.players.some((p) => p.slot === 3 && !p.connected));
    check(!!away, '3P가 끊기면 화면에 "다시 연결 중"으로 표시됨');
    const C3session = received(D, 'player_list_updated').map((l) => l.data.players.find((p) => p.slot === 3)).find(Boolean)?.session;
    const C3b = await controller(url, 'C3b', 'token-player-3');
    sockets.push(C3b);
    const back = await waitFor(C3b, 'player_assigned');
    check(back.slot === 3 && back.rejoined === true, '3P가 새로고침해도 같은 3P 자리로 돌아옴', back);
    // 새로고침한 폰은 영점 번호를 0부터 다시 세므로, 화면이 "새 연결"임을 알 수 있어야 합니다.
    const relist = await waitFor(D, 'player_list_updated', 2000, (d) => d.players.some((p) => p.slot === 3 && p.connected)).catch(() => null);
    const newSession = relist && relist.players.find((p) => p.slot === 3).session;
    D.log = [];
    C3b.emit('aim', { yaw: 1, pitch: 0, epoch: 1 });
    await sleep(200);
    const aimSession = received(D, 'aim')[0]?.data.session;
    check(typeof C3session === 'number' && typeof newSession === 'number' && newSession !== C3session && aimSession === newSession,
      '다시 들어온 3P는 새 연결 번호를 받고, 화면이 받는 조준에도 그 번호가 붙음', { before: C3session, after: newSession, aimSession });

    // 자리 예약 중에는 새 폰이 못 들어오고, 시간이 지나면 들어옴
    C[3].disconnect();
    await sleep(100);
    const C6 = await controller(url, 'C6', 'token-player-6');
    sockets.push(C6);
    const full2 = await waitFor(C6, 'room_full').catch(() => null);
    check(!!full2, '끊긴 4P 자리는 잠깐 예약돼서 새 폰은 거절됨');
    await sleep(RESERVE_MS + 300);
    C6.log = [];
    C6.emit('register_role', { role: 'controller', token: 'token-player-6' });
    const got = await waitFor(C6, 'player_assigned').catch(() => null);
    check(got && got.slot === 4, `예약 시간이 지나면 빈 4P 자리를 새 폰이 받음`, got);

    // 같은 토큰으로 다른 탭이 들어오면 자리를 넘겨받음
    const C1b = await controller(url, 'C1b', 'token-player-1');
    sockets.push(C1b);
    const take = await waitFor(C1b, 'player_assigned');
    const replaced = await waitFor(C[0], 'replaced').catch(() => null);
    D.log = [];
    C[0].emit('shoot', {});
    C1b.emit('shoot', {});
    await sleep(250);
    check(take.slot === 1 && !!replaced && received(D, 'shoot').length === 1, '같은 폰이 다시 붙으면 예전 연결은 쉬고 새 연결만 동작함', { slot: take.slot, replaced: !!replaced, shoots: received(D, 'shoot').length });

    // 화면 새로고침
    C3b.log = [];
    D.disconnect();
    const off = await waitFor(C3b, 'display_status', 2000, (d) => d.connected === false).catch(() => null);
    check(!!off, 'PC 화면이 꺼지면 폰이 알게 됨');
    const D2 = await display(url, 'display2');
    sockets.push(D2);
    const list2 = received(D2, 'player_list_updated')[0].data;
    const joined = await waitFor(C3b, 'display_joined').catch(() => null);
    check(list2.count === 4 && !!joined, '늦게 켠 PC 화면도 지금 플레이어 목록을 받고, 폰은 설정을 다시 보내라는 신호를 받음', { count: list2.count, joined: !!joined, log: C3b.log.map((l) => l.event) });

    // PC 화면을 하나 더 켜면 새 화면이 이어받고, 예전 화면은 쉬어야 합니다. (둘이 따로 게임을 돌리면 안 됨)
    C3b.log = [];
    const D3 = await display(url, 'display3');
    sockets.push(D3);
    const retired = await waitFor(D2, 'display_replaced', 1500).catch(() => null);
    D2.log = [];
    D3.log = [];
    C3b.log = [];
    C3b.emit('shoot', {});
    D2.emit('to_controllers', { event: 'game_state', data: { phase: 'lobby-from-old' } });
    D2.emit('to_slot', { slot: 3, event: 'ammo_update', data: { ammo: 1 } });
    await sleep(300);
    check(!!retired, '두 번째 PC 화면이 켜지면 첫 화면에 "다른 화면이 이어받았다"는 신호가 감');
    check(received(D3, 'shoot').length === 1 && received(D2, 'shoot').length === 0, '폰 입력은 새 화면에만 감', { new: received(D3, 'shoot').length, old: received(D2, 'shoot').length });
    check(received(C3b, 'game_state').length === 0 && received(C3b, 'ammo_update').length === 0, '쉬는 화면이 보낸 게임 상태·탄 정보는 폰에 안 감', C3b.log.map((l) => l.event));
    D2.disconnect();
    await sleep(250);
    check(!received(C3b, 'display_status').some((l) => l.data.connected === false), '쉬는 화면을 닫아도 폰은 "PC 꺼짐"으로 바뀌지 않음');
  } finally {
    sockets.forEach((s) => s.close());
    child.kill();
  }
}

// ---------------------------------------------------------------------------
// 2. 화면(브라우저) 로직 검사 + 스크린샷
// ---------------------------------------------------------------------------
function findChrome() {
  const candidates = [
    process.env.CHROME_PATH,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser'
  ].filter(Boolean);
  return candidates.find((p) => fs.existsSync(p));
}

// 실제 폰처럼 행동하는 가짜 컨트롤러
class SimPhone {
  constructor(sock) {
    this.s = sock;
    this.epoch = 0;
    this.sens = 1;
    this.weapon = 'pistol';
    this.calibrated = false;
    this.centering = true;
    this.yaw = 0;
    this.pitch = 0;
    sock.on('display_joined', () => this.sendSettings());
  }
  sendSettings() {
    this.s.emit('settings', { sens: this.sens, weapon: this.weapon, epoch: this.epoch, calibrated: this.calibrated, centering: this.centering });
  }
  center() {
    this.epoch += 1;
    this.calibrated = true;
    this.centering = false;
    this.yaw = 0;
    this.pitch = 0;
    this.s.emit('center_reset', { epoch: this.epoch });
  }
  aim(yaw, pitch) {
    this.yaw = yaw;
    this.pitch = pitch;
    this.s.volatile.emit('aim', { yaw, pitch, roll: 0, epoch: this.epoch });
  }
  lastAmmo() {
    const l = received(this.s, 'ammo_update');
    return l.length ? l[l.length - 1].data.ammo : null;
  }
}

async function streamAims(phones, ms, fn) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    const t = (Date.now() - t0) / 1000;
    phones.forEach((p, i) => {
      const v = fn(i, t);
      if (v) p.aim(v[0], v[1]);
    });
    await sleep(16);
  }
}

async function browserTests() {
  let puppeteer;
  try {
    puppeteer = require('puppeteer-core');
  } catch (e) {
    console.log('\n[화면] puppeteer-core가 없어 건너뜀');
    return;
  }
  const chromePath = findChrome();
  if (!chromePath) {
    console.log('\n[화면] Chrome을 찾지 못해 건너뜀 (CHROME_PATH로 지정 가능)');
    return;
  }
  console.log('\n[화면] 4명 조준·영점·탄약이 각자 따로 동작하는지');
  const { child, url } = await startServer(BROWSER_RESERVE_MS);
  const browser = await puppeteer.launch({
    executablePath: chromePath,
    headless: true,
    args: ['--use-angle=swiftshader', '--enable-unsafe-swiftshader', '--autoplay-policy=no-user-gesture-required', '--no-first-run']
  });
  const sockets = [];
  const shot = async (page, name) => {
    if (!SCREENS_DIR) return;
    fs.mkdirSync(SCREENS_DIR, { recursive: true });
    const file = path.join(SCREENS_DIR, `${name}.png`);
    await page.screenshot({ path: file });
    console.log(`  shot ${file}`);
  };
  try {
    // ----- Three.js CDN이 막혔을 때 -----
    const blocked = async (patterns) => {
      const pg = await browser.newPage();
      await pg.setViewport({ width: 1280, height: 720 });
      await pg.setRequestInterception(true);
      pg.on('request', (req) => (patterns.some((re) => re.test(req.url())) ? req.abort() : req.continue()));
      await pg.goto(url, { waitUntil: 'networkidle0' }).catch(() => {});
      await sleep(300);
      return pg;
    };
    let cdnPage = await blocked([/cdnjs\.cloudflare\.com\/.*three/]);
    const fallbackOk = await cdnPage.evaluate(() => !!(window.THREE && window.__game)).catch(() => false);
    check(fallbackOk, '첫 번째 CDN(cdnjs)이 막혀도 두 번째 CDN에서 Three.js를 받아 게임 화면이 뜸');
    await cdnPage.close();
    cdnPage = await blocked([/cdnjs\.cloudflare\.com\/.*three/, /jsdelivr\.net\/.*three/, /unpkg\.com\/.*three/]);
    const fatal = await cdnPage.evaluate(() => {
      const el = document.getElementById('fatalOverlay');
      return el && getComputedStyle(el).display !== 'none' ? el.innerText : '';
    }).catch(() => '');
    check(/불러오지 못했어요/.test(fatal), 'Three.js를 아예 못 받으면 빈 화면 대신 한국어 안내가 보임', fatal.slice(0, 40));
    await shot(cdnPage, 'display-0-cdn-error');
    await cdnPage.close();

    const page = await browser.newPage();
    page.on('pageerror', (err) => { failures++; console.log(`  FAIL 화면 JS 오류: ${err.message}`); });
    await page.setViewport({ width: 1920, height: 1080 });
    await page.goto(url, { waitUntil: 'networkidle0' });
    await page.waitForFunction(() => window.__game && document.querySelector('.qrImage').src.startsWith('data:'));
    await sleep(600);
    await shot(page, 'display-1-lobby-empty');
    await page.setViewport({ width: 1280, height: 720 });
    await sleep(300);
    await shot(page, 'display-1-lobby-empty-1280');
    await page.setViewport({ width: 1920, height: 1080 });

    // GPU 메모리: 좀비·피 파티클을 지우면 지오메트리도 같이 지워져야 함
    const mem = await page.evaluate(async () => {
      const wait = (ms) => new Promise((r) => setTimeout(r, ms));
      spawnBloodBurst(new THREE.Vector3(0, 1.5, -6), 4);
      await wait(900);
      const base = renderer.info.memory.geometries;
      for (let i = 0; i < 8; i++) createZombie(['walker', 'runner', 'crawler', 'spitter', 'brute', 'bloater'][i % 6]);
      for (let i = 0; i < 6; i++) spawnBloodBurst(new THREE.Vector3(0, 1.5, -6), 10);
      await wait(300);
      const mid = renderer.info.memory.geometries;
      clearEnemies();
      await wait(1000);
      return { base, mid, after: renderer.info.memory.geometries };
    });
    check(mem.mid > mem.base + 20 && mem.after <= mem.base, `좀비·피를 지우면 GPU 지오메트리도 지워짐 (처음 ${mem.base} → 좀비 8마리 ${mem.mid} → 지운 뒤 ${mem.after})`, mem);

    const phones = [];
    for (let i = 1; i <= 4; i++) {
      const s = await controller(url, `P${i}`, `browser-player-${i}`);
      sockets.push(s);
      await waitFor(s, 'player_assigned');
      const ph = new SimPhone(s);
      ph.sendSettings();
      phones.push(ph);
    }
    await page.waitForFunction(() => [1, 2, 3, 4].every((s) => window.__game.PLAYERS[s].present));
    const G = (fn, ...a) => page.evaluate(fn, ...a);

    // 1P, 2P, 3P는 가운데를 맞추고, 4P는 아직 맞추는 중
    phones.slice(0, 3).forEach((p) => p.center());
    await sleep(200);
    await streamAims(phones.slice(0, 2), 700, () => [12, 0]);
    let st = await G(() => [1, 2].map((s) => window.__game.PLAYERS[s].aimX));
    check(near(st[0], 0.5) && near(st[1], 0.5), '1P와 2P 둘 다 오른쪽 12°를 겨누면 같은 위치(0.5)', st);

    phones[1].center();
    await streamAims(phones.slice(0, 2), 700, (i) => (i === 0 ? [12, 0] : [0, 0]));
    st = await G(() => [1, 2].map((s) => ({ x: window.__game.PLAYERS[s].aimX, e: window.__game.PLAYERS[s].epoch })));
    check(near(st[0].x, 0.5) && near(st[1].x, 0), '2P가 가운데를 다시 맞춰도 1P 조준은 그대로', st);

    // 손떨림: 작은 노이즈를 넣어도 크로스헤어가 거의 안 흔들림
    const jitter = [];
    const tj = Date.now();
    while (Date.now() - tj < 1400) {
      phones[2].aim(-6 + (Math.random() - 0.5) * 0.6, 3 + (Math.random() - 0.5) * 0.6);
      await sleep(16);
      if (Date.now() - tj > 800) jitter.push(await G(() => window.__game.PLAYERS[3].drawX));
    }
    const spreadPx = (Math.max(...jitter) - Math.min(...jitter)) * 960;
    check(spreadPx < 8, `손떨림(±0.3°)을 넣어도 3P 크로스헤어 흔들림이 작음 (${spreadPx.toFixed(1)}px, 필터 없으면 약 12px)`);

    // 빠르게 돌리면 늦지 않게 따라옴: 0° -> 12°로 확 돌렸을 때 90%까지 걸리는 시간
    await streamAims([phones[2]], 400, () => [0, 0]);
    const ts = Date.now();
    let reachedMs = null;
    while (Date.now() - ts < 1000) {
      phones[2].aim(12, 0);
      await sleep(16);
      const x = await G(() => window.__game.PLAYERS[3].drawX);
      if (x >= 0.45) { reachedMs = Date.now() - ts; break; }
    }
    check(reachedMs !== null && reachedMs < 300, `빠르게 돌리면 화면 조준점이 금방 따라옴 (${reachedMs}ms)`);

    // 로비에서 쏴도 게임이 시작되지 않음
    phones[0].s.emit('shoot', {});
    await sleep(250);
    st = await G(() => ({ phase: window.__game.phase, ammo: window.__game.PLAYERS[1].ammo }));
    check(st.phase === 'lobby' && st.ammo === 12, '로비에서 쏘면 연습 사격만 하고 게임은 그대로', st);

    // 연습 화면 스크린샷 (4명이 서로 다른 곳을 겨눔)
    await streamAims(phones, 900, (i, t) => [[-16, 6], [9, 7], [-5, -6], [15, -4]][i].map((v, k) => v + Math.sin(t * 3 + i + k) * 0.5));
    phones[0].s.emit('shoot', {});
    phones[2].s.emit('shoot', {});
    await sleep(60);
    await shot(page, 'display-2-practice-4p');

    // 3P가 시작 -> 모두 전투
    phones.forEach((p) => { p.s.log = []; });
    phones[2].s.emit('start_game', {});
    await page.waitForFunction(() => window.__game.phase === 'playing', { timeout: 3000 }).catch(() => {});
    const gs = await Promise.all(phones.map((p) => waitFor(p.s, 'game_state', 1500, (d) => d.phase === 'playing').catch(() => null)));
    check((await G(() => window.__game.phase)) === 'playing' && gs.every(Boolean), '3P 폰에서 시작하면 4명 모두 전투 상태가 됨');

    for (let i = 0; i < 3; i++) {
      phones[0].s.emit('shoot', {});
      await sleep(40);
    }
    await sleep(300);
    st = await G(() => [1, 2, 3, 4].map((s) => window.__game.PLAYERS[s].ammo));
    check(JSON.stringify(st) === '[9,12,12,12]', '1P가 3발 쏘면 1P 탄만 9발, 나머지는 12발', st);
    check(phones[0].lastAmmo() === 9 && received(phones[1].s, 'ammo_update').every((l) => l.data.ammo === 12), '1P 폰은 9발을 받고 2P 폰은 1P 탄 정보를 안 받음');

    phones[1].s.emit('weapon_change', { weapon: 'shotgun' });
    phones[0].s.emit('reload', { method: 'swipe' });
    phones[3].sens = 1.5;
    phones[3].sendSettings();
    await sleep(300);
    st = await G(() => [1, 2, 3, 4].map((s) => ({ w: window.__game.PLAYERS[s].weapon, a: window.__game.PLAYERS[s].ammo, sens: window.__game.PLAYERS[s].sens })));
    check(st[1].w === 'shotgun' && st[1].a === 6 && st[0].w === 'pistol', '2P만 샷건으로 바뀜', st);
    check(st[0].a === 12, '1P 장전은 1P만 채움', st[0]);
    check(st[3].sens === 1.5 && st[0].sens === 1, '감도는 플레이어마다 따로 (4P만 1.5)', st.map((x) => x.sens));

    // 4P가 전투 중에 가운데를 맞춤
    phones[3].center();
    await sleep(200);

    // 전투 화면 스크린샷: 좀비가 나올 때까지 조준을 흔들며 기다림
    const t0 = Date.now();
    while (Date.now() - t0 < 9000) {
      const t = (Date.now() - t0) / 1000;
      phones.forEach((p, i) => p.aim(Math.sin(t * 0.9 + i * 1.7) * 14, Math.cos(t * 0.7 + i) * 6 - 2));
      if (Math.random() < 0.05) phones[Math.floor(Math.random() * 4)].s.emit('shoot', {});
      await sleep(16);
    }
    const zc = await G(() => window.__game.zombieCount);
    check(zc > 0, `전투가 시작되면 좀비가 나옴 (${zc}마리)`);
    // 심전도 선: 지나간 자리에 흐린 점이 남지 않아야 함 (진한 선 옆이 아닌 곳에 흐린 픽셀이 있으면 찌꺼기)
    const ecg = await G(() => {
      const c = document.getElementById('ecgCanvas');
      const W = c.width;
      const H = c.height;
      const d = c.getContext('2d').getImageData(0, 0, W, H).data;
      const A = (x, y) => (x < 0 || y < 0 || x >= W || y >= H ? 0 : d[(y * W + x) * 4 + 3]);
      let stray = 0;
      let drawn = 0;
      for (let y = 0; y < H; y++) {
        for (let x = 0; x < W; x++) {
          const a = A(x, y);
          if (!a) continue;
          drawn++;
          if (a >= 40) continue;
          let solid = false;
          for (let dy = -3; dy <= 3 && !solid; dy++) for (let dx = -3; dx <= 3 && !solid; dx++) if (A(x + dx, y + dy) >= 80) solid = true;
          if (!solid) stray++;
        }
      }
      return { stray, drawn };
    });
    check(ecg.drawn > 100 && ecg.stray === 0, `심전도 선이 지나간 자리에 흐린 찌꺼기가 안 남음 (찌꺼기 ${ecg.stray}픽셀)`, ecg);
    await shot(page, 'display-3-gameplay-4p');
    await page.setViewport({ width: 1280, height: 720 });
    await streamAims(phones, 300, (i, t) => [Math.sin(t + i) * 14, -2]);
    await shot(page, 'display-3-gameplay-4p-1280');
    await page.setViewport({ width: 1920, height: 1080 });

    // 전투 중 Esc로 참가 QR 열기
    await page.keyboard.press('Escape');
    await sleep(200);
    await shot(page, 'display-4-join-overlay');
    await page.keyboard.press('Escape');

    // PC 화면 새로고침: 폰은 그대로, 설정은 다시 받아옴
    phones.forEach((p) => { p.s.log = []; });
    await page.reload({ waitUntil: 'networkidle0' });
    await page.waitForFunction(() => window.__game && [1, 2, 3, 4].every((s) => window.__game.PLAYERS[s].present), { timeout: 5000 }).catch(() => {});
    await sleep(500);
    st = await G(() => ({ phase: window.__game.phase, sens4: window.__game.PLAYERS[4].sens, cal: [1, 2, 3, 4].map((s) => window.__game.PLAYERS[s].calibrated) }));
    check(st.phase === 'lobby' && st.sens4 === 1.5 && st.cal.every(Boolean), 'PC 화면을 새로고침해도 4명 그대로, 감도·영점 상태도 다시 받아옴', st);
    const lobbyAgain = await Promise.all(phones.map((p) => waitFor(p.s, 'game_state', 1500, (d) => d.phase === 'lobby').catch(() => null)));
    check(lobbyAgain.every(Boolean), '새로고침 뒤 폰들도 로비 상태를 받음');

    // ----- 폰 화면 (390x844) -----
    phones[3].s.close();
    await sleep(BROWSER_RESERVE_MS + 300);
    const phone = await browser.newPage();
    phone.on('pageerror', (err) => { failures++; console.log(`  FAIL 컨트롤러 JS 오류: ${err.message}`); });
    await phone.setViewport({ width: 390, height: 844, deviceScaleFactor: 2, isMobile: true, hasTouch: true });
    await phone.goto(`${url}/controller.html`, { waitUntil: 'networkidle0' });
    await sleep(300);
    const overflow = async (label) => {
      const bad = await phone.evaluate(() => {
        const W = window.innerWidth;
        const H = window.innerHeight;
        const out = [];
        document.querySelectorAll('.screen.on *, body.sheet-open #sheet *').forEach((el) => {
          const r = el.getBoundingClientRect();
          if (!r.width || !r.height) return;
          if (r.left < -1 || r.right > W + 1 || r.bottom > H + 1 || r.top < -1) out.push(`${el.tagName}.${el.className}`);
          const ox = getComputedStyle(el).overflowX;
          if ((ox === 'auto' || ox === 'scroll') && el.scrollWidth > el.clientWidth + 1) out.push(`scroll:${el.tagName}.${el.className}`);
        });
        if (document.documentElement.scrollWidth > W) out.push('page-x-scroll');
        return out;
      });
      check(bad.length === 0, `폰 화면이 넘치지 않음: ${label}`, bad.slice(0, 5));
    };
    await shot(phone, 'controller-1-intro');
    await overflow('시작');
    await phone.click('#btnActivate');
    await phone.waitForSelector('#scrCenter.on', { timeout: 3000 });
    // 가짜 방향 센서 값
    const orient = (alpha, beta, gamma) => phone.evaluate((a, b, g) => {
      window.dispatchEvent(new DeviceOrientationEvent('deviceorientation', { alpha: a, beta: b, gamma: g }));
    }, alpha, beta, gamma);
    for (let i = 0; i < 5; i++) { await orient(30, 5, 0); await sleep(20); }
    await sleep(400);
    await shot(phone, 'controller-2-center');
    await overflow('가운데 맞추기');
    await phone.tap('#scrCenter');
    await phone.waitForSelector('#scrPlay.on', { timeout: 3000 });
    // 오른쪽으로 10° 돌리면 (alpha가 줄어듦) 화면에서도 오른쪽
    for (let i = 0; i < 30; i++) { await orient(20, 5, 0); await sleep(16); }
    await sleep(300);
    const p4 = await G(() => window.__game.PLAYERS[4]);
    check(p4.present && p4.aimX > 0.3, `실제 컨트롤러 페이지: 폰을 오른쪽으로 10° 돌리면 크로스헤어도 오른쪽 (aimX=${p4.aimX.toFixed(2)})`);

    // 조준 보내기: 센서가 60Hz 근처에서 들쭉날쭉하게 들어와도 대부분 화면에 닿아야 함
    await G(() => { window.__aim4 = 0; socket.on('aim', (d) => { if (d.slot === 4) window.__aim4++; }); });
    const sent = await phone.evaluate(async () => {
      const wait = (ms) => new Promise((r) => setTimeout(r, ms));
      let n = 0;
      const t0 = performance.now();
      while (performance.now() - t0 < 1000) {
        window.dispatchEvent(new DeviceOrientationEvent('deviceorientation', { alpha: 25 + Math.sin(n / 5) * 4, beta: 5, gamma: 0 }));
        n++;
        await wait(n % 2 ? 13 : 19); // 평균 16ms, 매번 조금씩 흔들림
      }
      return n;
    });
    await sleep(300);
    const gotAims = await G(() => window.__aim4);
    check(gotAims >= sent * 0.8, `들쭉날쭉한 60Hz 조준 값도 거의 다 보냄 (센서 ${sent}번 → 화면 ${gotAims}번)`);
    // 마지막 값이 버려지지 않아야 함: 아주 짧은 간격으로 두 번 움직이면 두 번째 값이 화면에 남아야 함
    await phone.evaluate(() => {
      window.dispatchEvent(new DeviceOrientationEvent('deviceorientation', { alpha: 30, beta: 5, gamma: 0 }));
      window.dispatchEvent(new DeviceOrientationEvent('deviceorientation', { alpha: 18, beta: 5, gamma: 0 }));
    });
    await sleep(250);
    const lastYaw = await G(() => window.__game.PLAYERS[4].rawYaw);
    check(near(lastYaw, 12, 0.3), `잠깐 사이에 들어온 마지막 조준 값도 화면에 닿음 (yaw ${lastYaw})`);
    for (let i = 0; i < 10; i++) { await orient(30, 5, 0); await sleep(16); }
    await sleep(300);
    await shot(phone, 'controller-3-play-lobby');
    await overflow('연습(로비)');

    // 장전 오작동 검사: 쏜 직후에는 총구가 장전 각도(-38°)보다 크게 내려갔다 올라와도 장전이 안 됨
    await G(() => window.__game.startGame());
    await sleep(500);
    await phone.tap('#trigger');
    await sleep(60);
    await phone.tap('#trigger');
    // 쏘자마자 반동으로 총구가 -55°까지 꺾였다가 0.2초 안에 돌아옴 (센서 값은 페이지 안에서 정확한 간격으로 넣음)
    const tiltMinPitch = await phone.evaluate(async () => {
      const wait = (ms) => new Promise((r) => setTimeout(r, ms));
      const o = (b) => window.dispatchEvent(new DeviceOrientationEvent('deviceorientation', { alpha: 30, beta: b, gamma: 0 }));
      let min = 0;
      for (let i = 0; i < 10; i++) { o(-50); await wait(16); min = Math.min(min, pitch); }
      for (let i = 0; i < 4; i++) { o(5); await wait(16); }
      return min;
    });
    await sleep(300);
    st = await G(() => window.__game.PLAYERS[4].ammo);
    check(tiltMinPitch < -38 && st === 10, `두 번 쏜 직후 반동으로 총구가 ${tiltMinPitch.toFixed(0)}°까지 꺾여도 장전 안 됨 (탄 ${st}/12)`);
    await sleep(400);
    for (let i = 0; i < 12; i++) { await orient(30, -50, 0); await sleep(16); } // 총구를 확 내렸다가
    for (let i = 0; i < 6; i++) { await orient(30, 5, 0); await sleep(16); } // 다시 올림
    await sleep(300);
    st = await G(() => window.__game.PLAYERS[4].ammo);
    check(st === 12, `총구를 내렸다 올리면 장전됨 (탄 ${st}/12)`);
    await phone.tap('#trigger');
    await sleep(300);
    await shot(phone, 'controller-4-play-battle');
    await overflow('전투');
    await phone.tap('#btnSettings');
    await sleep(400);
    await shot(phone, 'controller-5-settings');
    await overflow('설정 시트');
    await phone.setViewport({ width: 360, height: 640, deviceScaleFactor: 2, isMobile: true, hasTouch: true });
    await sleep(300);
    await overflow('작은 폰(360x640) 설정 시트');
    await phone.tap('#btnCloseSheet');
    await sleep(300);
    await overflow('작은 폰(360x640) 전투');
    await shot(phone, 'controller-6-small-phone');
    await phone.setViewport({ width: 390, height: 844, deviceScaleFactor: 2, isMobile: true, hasTouch: true });
    await sleep(300);

    // ----- 방아쇠: 탭은 쏘고, 아래로 쓸면 쏘지 않고 장전만 -----
    await G(() => {
      window.__shots4 = 0;
      const wrap = (fn) => function (p) { if (p && p.slot === 4) window.__shots4++; return fn.apply(this, arguments); };
      performShoot = wrap(performShoot);
      performTestShoot = wrap(performTestShoot);
      window.__game.endGame();
      window.__game.startGame();
    });
    await sleep(400);
    const box = await phone.$eval('#trigger', (el) => { const r = el.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height * 0.3 }; });
    await phone.tap('#trigger');
    await sleep(200);
    st = await G(() => ({ shots: window.__shots4, ammo: window.__game.PLAYERS[4].ammo }));
    check(st.shots === 1 && st.ammo === 11, '방아쇠를 톡 치면 한 발 나감', st);
    await phone.touchscreen.touchStart(box.x, box.y);
    await sleep(160);
    const heldShots = await G(() => window.__shots4);
    await phone.touchscreen.touchEnd();
    await sleep(150);
    st = await G(() => ({ shots: window.__shots4, ammo: window.__game.PLAYERS[4].ammo }));
    check(heldShots === 2 && st.shots === 2, '방아쇠를 누르고 있으면 손을 떼기 전에 바로 한 발 나감 (뗄 때 또 안 나감)', { heldShots, ...st });
    await phone.touchscreen.touchStart(box.x, box.y);
    await phone.touchscreen.touchMove(box.x, box.y + 30);
    await phone.touchscreen.touchMove(box.x, box.y + 90);
    await phone.touchscreen.touchEnd();
    await sleep(300);
    st = await G(() => ({ shots: window.__shots4, ammo: window.__game.PLAYERS[4].ammo }));
    check(st.shots === 2 && st.ammo === 12, `아래로 쓸어 장전하면 총알이 안 나가고 장전만 됨 (사격 ${st.shots - 2}번, 탄 ${st.ammo}/12)`, st);

    // ----- 폰 새로고침: 같은 자리, 조준·무기·탄·좌우 반대 설정이 그대로 -----
    await phone.evaluate(() => document.querySelector('#weaponSeg [data-weapon="shotgun"]').click());
    await sleep(200);
    await phone.tap('#trigger');
    await sleep(200);
    await phone.evaluate(() => document.getElementById('btnInvert').click());
    // 가운데를 한 번 더 맞춰서 화면의 영점 번호를 2로 올려 둡니다. (새로고침한 폰은 1부터 다시 셈)
    await phone.evaluate(() => document.getElementById('btnRecenter').click());
    await sleep(450);
    await phone.tap('#scrCenter');
    await sleep(300);
    const before = await G(() => ({ w: window.__game.PLAYERS[4].weapon, a: window.__game.PLAYERS[4].ammo, e: window.__game.PLAYERS[4].epoch }));
    check(before.w === 'shotgun' && before.a === 5 && before.e >= 2, '새로고침 전: 4P 샷건 5/6발, 영점 번호 2 이상', before);
    await phone.reload({ waitUntil: 'networkidle0' });
    await phone.click('#btnActivate');
    await phone.waitForSelector('#scrCenter.on', { timeout: 3000 });
    for (let i = 0; i < 5; i++) { await orient(30, 5, 0); await sleep(20); }
    await sleep(400);
    await phone.tap('#scrCenter');
    await phone.waitForSelector('#scrPlay.on', { timeout: 3000 });
    for (let i = 0; i < 30; i++) { await orient(20, 5, 0); await sleep(16); }
    await sleep(300);
    const after = await G(() => { const p = window.__game.PLAYERS[4]; return { slot: p.slot, present: p.present, aimX: p.aimX, w: p.weapon, a: p.ammo }; });
    const phoneView = await phone.evaluate(() => ({ name: state.name, ammo: document.getElementById('ammoVal').textContent, weapon: document.getElementById('weaponName').textContent, invert: document.getElementById('invertSwitch').classList.contains('on') }));
    check(after.present && phoneView.name === '4P', '폰을 새로고침해도 같은 4P 자리로 돌아옴', phoneView.name);
    check(Math.abs(after.aimX) > 0.3, `폰 새로고침 뒤에도 조준이 화면에 반영됨 (가운데에 멈추지 않음, aimX=${after.aimX.toFixed(2)})`);
    check(after.w === 'shotgun' && after.a === 5 && phoneView.weapon === '샷건' && phoneView.ammo === '5', '폰 새로고침 뒤에도 무기·탄이 그대로 (화면 샷건 5발, 폰도 샷건 5발)', { display: { w: after.w, a: after.a }, phone: phoneView });
    check(phoneView.invert && after.aimX < -0.3, '폰 새로고침 뒤에도 "좌우 반대로"가 켜져 있어서 오른쪽으로 돌리면 왼쪽으로 감', { invert: phoneView.invert, aimX: after.aimX });
    for (let i = 0; i < 10; i++) { await orient(30, 5, 0); await sleep(16); }

    // 5번째 폰 화면
    const p5 = await browser.newPage();
    await p5.setViewport({ width: 390, height: 844, deviceScaleFactor: 2, isMobile: true, hasTouch: true });
    const extra = await controller(url, 'P5', 'browser-player-5x');
    sockets.push(extra);
    await waitFor(extra, 'room_full').catch(() => null);
    await p5.goto(`${url}/controller.html`, { waitUntil: 'networkidle0' });
    await p5.click('#btnActivate');
    const fullShown = await p5.waitForSelector('#scrFull.on', { timeout: 3000 }).then(() => true).catch(() => false);
    check(fullShown, '5번째 폰 화면에 "4명이 꽉 찼어요"가 보임');
    await shot(p5, 'controller-7-room-full');

    // PC 화면을 하나 더 열면 새 화면이 이어받고, 예전 화면은 쉰다는 안내를 보여 줌
    const page2 = await browser.newPage();
    page2.on('pageerror', (err) => { failures++; console.log(`  FAIL 두 번째 화면 JS 오류: ${err.message}`); });
    await page2.setViewport({ width: 1280, height: 720 });
    await page2.goto(url, { waitUntil: 'networkidle0' });
    await page2.waitForFunction(() => window.__game && window.__game.PLAYERS[4].present, { timeout: 4000 }).catch(() => {});
    const oldRetired = await page.waitForFunction(() => document.body.classList.contains('retired'), { timeout: 3000 }).then(() => true).catch(() => false);
    check(oldRetired, '두 번째 PC 화면을 열면 첫 화면에 "다른 화면에서 게임을 열었어요"가 보임');
    check((await page2.evaluate(() => window.__game.PLAYERS[4].present)), '두 번째 화면이 지금 플레이어들을 그대로 이어받음');
    await page.bringToFront();
    await shot(page, 'display-5-replaced');

    await midJoinTests(browser, shot);
  } finally {
    sockets.forEach((s) => s.close());
    await browser.close();
    child.kill();
  }
}

// 1P 혼자 시작한 뒤, 게임 도중에 2P·3P·4P가 QR로 들어오는 경우
async function midJoinTests(browser, shot) {
  console.log('\n[화면] 게임 중간에 2P·3P·4P가 들어와도 바로 함께 싸우는지');
  const { child, url } = await startServer(BROWSER_RESERVE_MS);
  const sockets = [];
  const page = await browser.newPage();
  page.on('pageerror', (err) => { failures++; console.log(`  FAIL 화면 JS 오류: ${err.message}`); });
  try {
    await page.setViewport({ width: 1920, height: 1080 });
    await page.goto(url, { waitUntil: 'networkidle0' });
    await page.waitForFunction(() => window.__game && document.querySelector('.qrImage').src.startsWith('data:'));
    const G = (fn, ...a) => page.evaluate(fn, ...a);
    const miniState = () => G(() => {
      const el = document.getElementById('miniJoin');
      const img = el.querySelector('.qrImage');
      return {
        shown: getComputedStyle(el).display !== 'none',
        next: document.getElementById('miniNext').textContent,
        left: el.querySelector('.slotsLeft').textContent,
        qr: img.src.startsWith('data:') && img.getBoundingClientRect().width >= 100
      };
    });

    const join = async (n) => {
      const sock = await controller(url, `M${n}`, `mid-player-${n}`);
      sockets.push(sock);
      const a = await waitFor(sock, 'player_assigned');
      const ph = new SimPhone(sock);
      ph.sendSettings();
      return { ph, slot: a.slot };
    };

    const p1 = await join(1);
    p1.ph.center();
    await page.waitForFunction(() => window.__game.PLAYERS[1].calibrated, { timeout: 3000 }).catch(() => {});
    check(!(await miniState()).shown, '로비에서는 작은 참가 QR이 안 보임 (오른쪽 큰 QR만 보임)');
    p1.ph.s.emit('start_game', {});
    await page.waitForFunction(() => window.__game.phase === 'playing', { timeout: 3000 }).catch(() => {});
    check((await G(() => window.__game.phase)) === 'playing', '1P 혼자서도 게임을 시작할 수 있음');

    let mini = await miniState();
    check(mini.shown && mini.qr && mini.next === '2P' && mini.left === '3', `게임 중 화면에 참가 QR이 늘 보이고 "2P로 참가", 남은 자리 3 (${JSON.stringify(mini)})`);

    // 게임 도중 2P 참가
    const p2 = await join(2);
    check(p2.slot === 2, '게임 중에 들어온 폰이 2P 자리를 받음', p2.slot);
    const gs2 = await waitFor(p2.ph.s, 'game_state', 2000, (d) => d.phase === 'playing').catch(() => null);
    check(!!gs2, '2P 폰은 들어오자마자 "전투 중" 상태를 받음');
    await page.waitForFunction(() => window.__game.PLAYERS[2].present, { timeout: 3000 }).catch(() => {});
    const card2 = await G(() => document.querySelectorAll('.pcard')[1].className);
    check(!/empty/.test(card2), '화면 아래 2P 카드가 빈자리에서 참가로 바뀜', card2);
    mini = await miniState();
    check(mini.next === '3P' && mini.left === '2', `참가 QR이 다음 사람을 "3P"로 안내함 (${mini.next}, 남은 자리 ${mini.left})`);

    // 2P가 가운데를 맞추고 쏘면 실제 전투 사격(탄이 줄어듦)이 됨. 1P 탄은 그대로.
    p2.ph.center();
    await page.waitForFunction(() => window.__game.PLAYERS[2].calibrated, { timeout: 3000 }).catch(() => {});
    await streamAims([p1.ph, p2.ph], 600, (i, t) => (i === 0 ? [-10, 3] : [9 + Math.sin(t * 4), 2]));
    p2.ph.s.emit('shoot', {});
    p2.ph.s.emit('shoot', {});
    await sleep(300);
    const ammo = await G(() => [1, 2].map((s) => window.__game.PLAYERS[s].ammo));
    check(ammo[0] === 12 && ammo[1] === 10, `중간에 들어온 2P가 쏘면 2P 탄만 줄어듦 (1P ${ammo[0]}, 2P ${ammo[1]})`);
    check(p2.ph.lastAmmo() === 10, '2P 폰도 남은 탄 10발을 받음', p2.ph.lastAmmo());

    // 좀비가 나올 때까지 기다렸다가 스크린샷 (2명 + 오른쪽 참가 QR)
    const t0 = Date.now();
    while (Date.now() - t0 < 6000) {
      const t = (Date.now() - t0) / 1000;
      p1.ph.aim(Math.sin(t * 0.9) * 12, Math.cos(t * 0.7) * 5 - 2);
      p2.ph.aim(Math.sin(t * 0.8 + 2) * 12, Math.cos(t * 0.6 + 1) * 5 - 1);
      await sleep(16);
    }
    await shot(page, 'display-6-midjoin-2p');
    await page.setViewport({ width: 1280, height: 720 });
    await sleep(400);
    await shot(page, 'display-6-midjoin-2p-1280');
    await page.setViewport({ width: 1920, height: 1080 });

    // 3P, 4P도 게임 도중 참가
    const p3 = await join(3);
    const p4 = await join(4);
    check(p3.slot === 3 && p4.slot === 4, '게임 중에 3P, 4P도 차례로 자리를 받음', [p3.slot, p4.slot]);
    await page.waitForFunction(() => [1, 2, 3, 4].every((s) => window.__game.PLAYERS[s].present), { timeout: 3000 }).catch(() => {});
    p3.ph.center();
    p4.ph.center();
    await sleep(300);
    p4.ph.s.emit('shoot', {});
    await sleep(300);
    const a4 = await G(() => window.__game.PLAYERS[4].ammo);
    check(a4 === 11, `중간에 들어온 4P도 바로 쏠 수 있음 (탄 ${a4})`);
    mini = await miniState();
    check(!mini.shown, '4명이 꽉 차면 게임 중 참가 QR이 사라짐');
    check((await G(() => window.__game.phase)) === 'playing', '여러 명이 중간에 들어와도 게임은 계속 진행됨');
  } finally {
    sockets.forEach((s) => s.close());
    await page.close();
    child.kill();
  }
}

(async () => {
  try {
    await serverTests();
    if (!NO_BROWSER) await browserTests();
  } catch (err) {
    failures++;
    console.error('\n오류:', err);
  }
  console.log(`\n결과: ${passes}개 통과, ${failures}개 실패`);
  process.exit(failures ? 1 : 0);
})();
