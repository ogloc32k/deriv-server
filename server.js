const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ---------- SSE ----------
const sseClients = new Set();
let logId = 1;

function addLog(msg) {
  const entry = { id: logId++, time: new Date().toISOString(), message: msg };
  state.logs.unshift(entry);
  if (state.logs.length > 200) state.logs.pop();
  broadcastSSE({ logs: [entry], state: sanitizeState() });
}

function broadcastSSE(payload) {
  sseClients.forEach(c => c.write(`data: ${JSON.stringify(payload)}\n\n`));
}

function sanitizeState() {
  const { logs, ...rest } = state;
  return rest;
}

// ---------- Markets ----------
const MARKETS = {
  "R_10":  { name: "Volatility 10 Index",  dp: 3 },
  "R_25":  { name: "Volatility 25 Index",  dp: 3 },
  "R_50":  { name: "Volatility 50 Index",  dp: 4 },
  "R_75":  { name: "Volatility 75 Index",  dp: 4 },
  "R_100": { name: "Volatility 100 Index", dp: 2 }
};

// ---------- State ----------
const state = {
  active: false,
  marketSymbol: 'R_100',
  dp: MARKETS['R_100'].dp,
  balance: null,
  currency: 'USD',
  // Settings
  minConfidence: 45,          // 45% confidence threshold
  maxOverBarrier: 4,
  minUnderBarrier: 6,
  dailyStopLossPct: 10,      // % of initial daily balance
  dailyTakeProfitPct: 20,    // % of initial daily balance
  riskPct: 2,                // risk per trade as % of balance
  // Runtime
  dailyStartBalance: null,
  dailyPnl: 0,
  locked: false,
  waiting: false,
  pendingContractId: null,
  balanceBefore: null,
  timer: null,
  signalConfidence: 0,
  signalDirection: null,
  signalBarrier: null,
  logs: []
};

// ---------- Analyzer (z‑score based) ----------
class Analyzer {
  constructor() {
    this.ticks = [];
    this.count = 0;
    this.mean = new Array(10).fill(0);
    this.m2 = new Array(10).fill(0);
  }

  feed(price, dp) {
    const digit = parseInt(parseFloat(price).toFixed(dp).slice(-1));
    this.ticks.push(digit);
    if (this.ticks.length > 1000) this.ticks.shift();
    if (this.ticks.length >= 100) {
      const recent = this.ticks.slice(-100);
      const freq = {};
      for (let d = 0; d < 10; d++) freq[d] = recent.filter(x => x === d).length / 100;
      this.count++;
      for (let d = 0; d < 10; d++) {
        const delta = freq[d] - this.mean[d];
        this.mean[d] += delta / this.count;
        this.m2[d] += delta * (freq[d] - this.mean[d]);
      }
    }
  }

  getZ() {
    if (this.count < 5) return null;
    const recent = this.ticks.slice(-100);
    const freq = {};
    for (let d = 0; d < 10; d++) freq[d] = recent.filter(x => x === d).length / 100;
    const z = {};
    for (let d = 0; d < 10; d++) {
      const variance = this.m2[d] / (this.count - 1);
      const std = Math.sqrt(variance) || 0.01;
      z[d] = (freq[d] - this.mean[d]) / std;
    }
    return z;
  }
}

const analyzer = new Analyzer();

// ---------- Confidence ----------
function computeConfidence(z) {
  if (!z) return { over: 0, under: 0 };
  let over = 0, under = 0;
  if (z[0] < 0 && z[1] < 0) {
    const rare = Math.min(3, Math.max(0, -Math.min(z[0], z[1]))) / 3;
    over = (rare * 0.5 + 0.5) * 100;
  }
  if (z[8] < 0 && z[9] < 0) {
    const rare = Math.min(3, Math.max(0, -Math.min(z[8], z[9]))) / 3;
    under = (rare * 0.5 + 0.5) * 100;
  }
  return { over, under };
}

function selectBarrier(direction) {
  const freq = {};
  const recent = analyzer.ticks.slice(-100);
  for (let d = 0; d < 10; d++) freq[d] = recent.filter(x => x === d).length / 100;

  let bestBarrier = null, bestProfit = -Infinity;
  if (direction === 'over') {
    for (let n = 0; n <= state.maxOverBarrier; n++) {
      let win = 0;
      for (let d = n + 1; d <= 9; d++) win += freq[d];
      const profit = win * (10 / (9 - n)) * 0.98 - 1;
      if (profit > bestProfit) { bestProfit = profit; bestBarrier = n; }
    }
  } else {
    for (let n = state.minUnderBarrier; n <= 9; n++) {
      let win = 0;
      for (let d = 0; d < n; d++) win += freq[d];
      const profit = win * (10 / n) * 0.98 - 1;
      if (profit > bestProfit) { bestProfit = profit; bestBarrier = n; }
    }
  }
  return bestBarrier;
}

// ---------- Risk management ----------
function dailyLimitReached() {
  if (!state.dailyStartBalance) return false;
  const pnlPct = (state.dailyPnl / state.dailyStartBalance) * 100;
  if (pnlPct <= -state.dailyStopLossPct) {
    addLog(`Daily stop loss reached (${pnlPct.toFixed(1)}%). Locked for the day.`);
    return true;
  }
  if (pnlPct >= state.dailyTakeProfitPct) {
    addLog(`Daily profit target reached (${pnlPct.toFixed(1)}%). Locked for the day.`);
    return true;
  }
  return false;
}

function calcStake() {
  const stake = (state.riskPct / 100) * state.balance;
  return Math.max(0.35, Math.round(stake * 100) / 100);
}

// ---------- SSE ----------
app.get('/api/logs', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
  res.write('\n');
  sseClients.add(res);
  res.write(`data: ${JSON.stringify({ state: sanitizeState() })}\n\n`);
  req.on('close', () => sseClients.delete(res));
});

app.get('/api/state', (req, res) => res.json({ ...state, logs: undefined }));

app.post('/api/control', (req, res) => {
  const { action } = req.body;
  if (action === 'start') {
    state.active = true;
    state.locked = false;
    state.dailyStartBalance = state.balance;
    state.dailyPnl = 0;
    state.waiting = false;
    addLog('Trading started.');
  } else if (action === 'stop') {
    state.active = false;
    if (state.timer) clearTimeout(state.timer);
    addLog('Trading stopped.');
  } else if (action === 'update') {
    const { marketSymbol, minConfidence, dailyStopLossPct, dailyTakeProfitPct, riskPct, maxOverBarrier, minUnderBarrier } = req.body;
    if (marketSymbol && MARKETS[marketSymbol]) {
      state.marketSymbol = marketSymbol;
      state.dp = MARKETS[marketSymbol].dp;
      if (derivWs && derivWs.readyState === WebSocket.OPEN) {
        send({ ticks: state.marketSymbol, req_id: ++reqId });
      }
    }
    if (minConfidence) state.minConfidence = parseInt(minConfidence);
    if (dailyStopLossPct) state.dailyStopLossPct = parseFloat(dailyStopLossPct);
    if (dailyTakeProfitPct) state.dailyTakeProfitPct = parseFloat(dailyTakeProfitPct);
    if (riskPct) state.riskPct = parseFloat(riskPct);
    if (maxOverBarrier !== undefined) state.maxOverBarrier = parseInt(maxOverBarrier);
    if (minUnderBarrier !== undefined) state.minUnderBarrier = parseInt(minUnderBarrier);
  }
  broadcastSSE({ state: sanitizeState() });
  res.json({ success: true });
});

// ---------- Deriv WebSocket ----------
let derivWs = null;
let reqId = 0;

function send(msg) { if (derivWs && derivWs.readyState === WebSocket.OPEN) derivWs.send(JSON.stringify(msg)); }

function connectDeriv() {
  if (derivWs) derivWs.close();
  const appId = process.env.DERIV_APP_ID;
  derivWs = new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${appId}`);
  derivWs.on('open', () => {
    addLog('Connected. Authorizing...');
    send({ authorize: process.env.DERIV_API_TOKEN });
  });
  derivWs.on('message', data => {
    try { handleMessage(JSON.parse(data)); } catch (e) { console.error('Invalid message'); }
  });
  derivWs.on('close', () => setTimeout(connectDeriv, 5000));
  derivWs.on('error', err => addLog(`WebSocket error: ${err.message}`));
}

function handleMessage(msg) {
  if (msg.error) return addLog(`Deriv error: ${msg.error.message}`);
  if (msg.msg_type === 'authorize') {
    addLog('Authorized. Subscribing...');
    send({ balance: 1, subscribe: 1, req_id: ++reqId });
    send({ ticks: state.marketSymbol, req_id: ++reqId });
  } else if (msg.msg_type === 'balance') {
    state.balance = msg.balance.balance;
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  } else if (msg.msg_type === 'tick') {
    const price = msg.tick.quote;
    if (msg.tick.symbol !== state.marketSymbol) return;
    analyzer.feed(price, state.dp);
    const z = analyzer.getZ();
    if (z) {
      const conf = computeConfidence(z);
      state.signalConfidence = Math.max(conf.over, conf.under);
      if (conf.over > conf.under && conf.over >= state.minConfidence) {
        state.signalDirection = 'over';
        state.signalBarrier = selectBarrier('over');
      } else if (conf.under > conf.over && conf.under >= state.minConfidence) {
        state.signalDirection = 'under';
        state.signalBarrier = selectBarrier('under');
      } else {
        state.signalDirection = null;
        state.signalBarrier = null;
      }
    }

    broadcastSSE({ state: sanitizeState() });

    // Trading logic
    if (!state.active || state.waiting || state.locked || !state.signalDirection) return;
    if (dailyLimitReached()) { state.locked = true; return; }

    state.waiting = true;
    const stake = calcStake();
    const contractType = state.signalDirection === 'over' ? 'DIGITOVER' : 'DIGITUNDER';
    const barrier = state.signalBarrier;
    state.balanceBefore = state.balance;
    addLog(`Signal ${state.signalConfidence}% – Placing ${contractType} barrier ${barrier}, stake ${stake.toFixed(2)}`);
    send({ proposal: 1, amount: stake, basis: 'stake', currency: state.currency, duration: 1, duration_unit: 't', symbol: state.marketSymbol, contract_type: contractType, barrier: barrier, req_id: ++reqId });
  } else if (msg.msg_type === 'proposal') {
    send({ buy: msg.proposal.id, price: msg.proposal.ask_price, req_id: ++reqId });
  } else if (msg.msg_type === 'buy') {
    state.pendingContractId = msg.buy.contract_id;
    addLog(`Bought: Win payout if the last digit of ${MARKETS[state.marketSymbol].name} is strictly ${state.signalDirection === 'over' ? 'higher' : 'lower'} than ${state.signalBarrier} after 1 ticks. (ID: ${msg.buy.contract_id})`);
    if (state.timer) clearTimeout(state.timer);
    state.timer = setTimeout(() => settleTrade(), 15000);
  }
}

function settleTrade() {
  if (!state.waiting) return;
  const diff = state.balance - state.balanceBefore;
  state.dailyPnl += diff;
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19) + ' GMT';
  addLog(now);
  if (diff > 0) addLog(`Profit: +${diff.toFixed(2)} USD`);
  else if (diff < 0) addLog(`Loss: ${diff.toFixed(2)} USD`);
  else addLog('Draw');
  addLog(`Daily P&L: ${state.dailyPnl.toFixed(2)} USD (${((state.dailyPnl/state.dailyStartBalance)*100).toFixed(1)}%)`);
  state.waiting = false;
  state.pendingContractId = null;
  state.balanceBefore = null;
  if (dailyLimitReached()) state.locked = true;
  broadcastSSE({ state: sanitizeState() });
}

connectDeriv();
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
