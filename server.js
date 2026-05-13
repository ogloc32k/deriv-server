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

// ---------- Multi‑timeframe Analyzer ----------
class Analyzer {
  constructor() {
    // Short‑term (100 ticks)
    this.shortTicks = [];
    this.shortCount = 0;
    this.shortMean = new Array(10).fill(0);
    this.shortM2 = new Array(10).fill(0);
    // Long‑term (500 ticks)
    this.longTicks = [];
    this.longCount = 0;
    this.longMean = new Array(10).fill(0);
    this.longM2 = new Array(10).fill(0);
    // Price history for trend
    this.prices = [];
    this.lastPrice = null;
  }

  feed(price, dp) {
    const digit = parseInt(parseFloat(price).toFixed(dp).slice(-1));
    // Short window
    this.shortTicks.push(digit);
    if (this.shortTicks.length > 1000) this.shortTicks.shift();
    if (this.shortTicks.length >= 100) {
      const recent = this.shortTicks.slice(-100);
      const freq = {};
      for (let d = 0; d < 10; d++) freq[d] = recent.filter(x => x === d).length / 100;
      this.shortCount++;
      for (let d = 0; d < 10; d++) {
        const delta = freq[d] - this.shortMean[d];
        this.shortMean[d] += delta / this.shortCount;
        this.shortM2[d] += delta * (freq[d] - this.shortMean[d]);
      }
    }
    // Long window
    this.longTicks.push(digit);
    if (this.longTicks.length > 2000) this.longTicks.shift();
    if (this.longTicks.length >= 500) {
      const recent = this.longTicks.slice(-500);
      const freq = {};
      for (let d = 0; d < 10; d++) freq[d] = recent.filter(x => x === d).length / 500;
      this.longCount++;
      for (let d = 0; d < 10; d++) {
        const delta = freq[d] - this.longMean[d];
        this.longMean[d] += delta / this.longCount;
        this.longM2[d] += delta * (freq[d] - this.longMean[d]);
      }
    }
    // Price trend
    this.prices.push(price);
    if (this.prices.length > 500) this.prices.shift();
    this.lastPrice = price;
  }

  getAnalysis() {
    // Need enough data for both timeframes
    if (this.shortCount < 5 || this.longCount < 2) return null;

    // Short‑term current freq
    const shortRecent = this.shortTicks.slice(-100);
    const shortFreq = {};
    for (let d = 0; d < 10; d++) shortFreq[d] = shortRecent.filter(x => x === d).length / 100;

    // Long‑term current freq
    const longRecent = this.longTicks.slice(-500);
    const longFreq = {};
    for (let d = 0; d < 10; d++) longFreq[d] = longRecent.filter(x => x === d).length / 500;

    // Z‑scores
    const zShort = {}, zLong = {};
    for (let d = 0; d < 10; d++) {
      const vs = this.shortM2[d] / (this.shortCount - 1), ss = Math.sqrt(vs) || 0.01;
      zShort[d] = (shortFreq[d] - this.shortMean[d]) / ss;
      const vl = this.longM2[d] / (this.longCount - 1), sl = Math.sqrt(vl) || 0.01;
      zLong[d] = (longFreq[d] - this.longMean[d]) / sl;
    }

    // Confidence (over 1, under 8) – both timeframes must agree
    let over = 0, under = 0;
    if (zShort[0] < 0 && zShort[1] < 0 && zLong[0] < 0 && zLong[1] < 0) {
      const rareShort = Math.min(3, Math.max(0, -Math.min(zShort[0], zShort[1]))) / 3;
      const rareLong = Math.min(3, Math.max(0, -Math.min(zLong[0], zLong[1]))) / 3;
      over = ((rareShort * 0.4 + rareLong * 0.6) + 0.4) * 100; // weighted
    }
    if (zShort[8] < 0 && zShort[9] < 0 && zLong[8] < 0 && zLong[9] < 0) {
      const rareShort = Math.min(3, Math.max(0, -Math.min(zShort[8], zShort[9]))) / 3;
      const rareLong = Math.min(3, Math.max(0, -Math.min(zLong[8], zLong[9]))) / 3;
      under = ((rareShort * 0.4 + rareLong * 0.6) + 0.4) * 100;
    }

    // Price trend slope (last 20 ticks)
    let priceSlope = 0;
    if (this.prices.length >= 20) {
      const recentPrices = this.prices.slice(-20).map(Number);
      const n = recentPrices.length;
      let sx = 0, sy = 0, sxy = 0, sx2 = 0;
      for (let i = 0; i < n; i++) {
        sx += i;
        sy += recentPrices[i];
        sxy += i * recentPrices[i];
        sx2 += i * i;
      }
      priceSlope = (n * sxy - sx * sy) / (n * sx2 - sx * sx);
    }

    // Optimal barrier selection
    const bestDirection = over >= under ? 'over' : 'under';
    const bestConf = Math.max(over, under);
    const barrier = this.selectBarrier(bestDirection, shortFreq);

    return {
      overConf: over,
      underConf: under,
      bestDirection,
      bestConf,
      barrier,
      priceSlope,
      zShort,
      zLong
    };
  }

  selectBarrier(direction, freq) {
    let bestBarrier = null, bestProfit = -Infinity;
    const maxOver = 4, minUnder = 6;
    if (direction === 'over') {
      for (let n = 0; n <= maxOver; n++) {
        let win = 0;
        for (let d = n + 1; d <= 9; d++) win += freq[d] || 0;
        const profit = win * (10 / (9 - n)) * 0.98 - 1;
        if (profit > bestProfit) { bestProfit = profit; bestBarrier = n; }
      }
    } else {
      for (let n = minUnder; n <= 9; n++) {
        let win = 0;
        for (let d = 0; d < n; d++) win += freq[d] || 0;
        const profit = win * (10 / n) * 0.98 - 1;
        if (profit > bestProfit) { bestProfit = profit; bestBarrier = n; }
      }
    }
    return bestBarrier;
  }
}

const analyzers = {};
for (const sym of Object.keys(MARKETS)) {
  analyzers[sym] = new Analyzer();
}

// ---------- State ----------
const state = {
  active: false,
  balance: null,
  currency: 'USD',
  // Settings
  minConfidence: 45,
  riskPct: 1.5,
  dailyStopLossPct: 6,
  dailyTakeProfitPct: 20,
  maxConcurrent: 2,
  trendFilterEnabled: true,
  // Runtime
  dailyStartBalance: null,
  dailyPnl: 0,
  locked: false,
  activeTrades: [],
  // Confidence display data
  marketSignals: {},   // { R_10: { over: 42, under: 23, trend: 'up' }, ... }
  logs: []
};

// ---------- Helpers ----------
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
  if (!state.balance) return 0.35;
  const stake = (state.riskPct / 100) * state.balance;
  return Math.max(0.35, Math.round(stake * 100) / 100);
}

function canOpenTrade(market) {
  return state.activeTrades.length < state.maxConcurrent &&
         !state.activeTrades.some(t => t.market === market);
}

function settleTrade(trade) {
  if (!state.active) return;
  const diff = state.balance - trade.balanceBefore;
  state.dailyPnl += diff;
  const result = diff > 0 ? 'WIN' : (diff < 0 ? 'LOSS' : 'DRAW');
  addLog(`${MARKETS[trade.market].name} ${trade.direction} barrier ${trade.barrier}: ${result} ${diff.toFixed(2)}`);
  addLog(`Daily P&L: ${state.dailyPnl.toFixed(2)} USD (${((state.dailyPnl/state.dailyStartBalance)*100).toFixed(1)}%)`);

  state.activeTrades = state.activeTrades.filter(t => t !== trade);
  if (dailyLimitReached()) state.locked = true;
  broadcastSSE({ state: sanitizeState() });
}

// ---------- SSE / API ----------
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
    state.activeTrades = [];
    addLog('Professional trading started.');
  } else if (action === 'stop') {
    state.active = false;
    for (const t of state.activeTrades) clearTimeout(t.timer);
    state.activeTrades = [];
    addLog('Trading stopped.');
  } else if (action === 'update') {
    const { minConfidence, riskPct, dailyStopLossPct, dailyTakeProfitPct, maxConcurrent, trendFilterEnabled } = req.body;
    if (minConfidence !== undefined) state.minConfidence = parseInt(minConfidence);
    if (riskPct !== undefined) state.riskPct = parseFloat(riskPct);
    if (dailyStopLossPct !== undefined) state.dailyStopLossPct = parseFloat(dailyStopLossPct);
    if (dailyTakeProfitPct !== undefined) state.dailyTakeProfitPct = parseFloat(dailyTakeProfitPct);
    if (maxConcurrent !== undefined) state.maxConcurrent = parseInt(maxConcurrent);
    if (trendFilterEnabled !== undefined) state.trendFilterEnabled = trendFilterEnabled === true || trendFilterEnabled === 'true';
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
    addLog('Authorized. Subscribing to balance & all ticks.');
    send({ balance: 1, subscribe: 1, req_id: ++reqId });
    for (const sym of Object.keys(MARKETS)) {
      send({ ticks_history: sym, count: 1000, end: 'latest', req_id: ++reqId });
    }
  }
  else if (msg.msg_type === 'balance') {
    state.balance = msg.balance.balance;
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'history') {
    const sym = msg.echo_req.ticks_history;
    const analyzer = analyzers[sym];
    if (analyzer && msg.history && msg.history.prices) {
      for (const p of msg.history.prices) {
        analyzer.feed(p, MARKETS[sym].dp);
      }
      send({ ticks: sym, req_id: ++reqId });
    }
  }
  else if (msg.msg_type === 'tick') {
    const sym = msg.tick.symbol;
    if (!MARKETS[sym]) return;
    const price = msg.tick.quote;
    const market = MARKETS[sym];
    const analyzer = analyzers[sym];
    analyzer.feed(price, market.dp);

    // Update confidence display
    const analysis = analyzer.getAnalysis();
    if (analysis) {
      const trend = analysis.priceSlope > 0.0001 ? 'up' : (analysis.priceSlope < -0.0001 ? 'down' : 'flat');
      state.marketSignals[sym] = {
        over: analysis.overConf.toFixed(1),
        under: analysis.underConf.toFixed(1),
        trend,
        bestDirection: analysis.bestDirection,
        bestConf: analysis.bestConf.toFixed(1)
      };
    }

    broadcastSSE({ state: sanitizeState() });

    // Trading logic
    if (!state.active || state.locked) return;
    if (!analysis || analysis.bestConf < state.minConfidence) return;
    if (analysis.barrier === null) return;

    // Trend filter
    if (state.trendFilterEnabled) {
      const slope = analysis.priceSlope;
      if (analysis.bestDirection === 'over' && slope < -0.0001) return; // must be up/flat
      if (analysis.bestDirection === 'under' && slope > 0.0001) return; // must be down/flat
    }

    if (state.activeTrades.some(t => t.market === sym)) return;
    if (dailyLimitReached()) { state.locked = true; return; }
    if (!canOpenTrade(sym)) return;

    // Open trade
    const stake = calcStake();
    const direction = analysis.bestDirection;
    const barrier = analysis.barrier;
    const contractType = direction === 'over' ? 'DIGITOVER' : 'DIGITUNDER';
    const trade = {
      market: sym,
      direction,
      barrier,
      stake,
      balanceBefore: state.balance,
      timer: null
    };
    state.activeTrades.push(trade);
    addLog(`${market.name} signal ${analysis.bestConf.toFixed(0)}% – ${contractType} barrier ${barrier}, stake ${stake.toFixed(2)}`);
    send({
      proposal: 1,
      amount: stake,
      basis: 'stake',
      currency: state.currency || 'USD',
      duration: 1,
      duration_unit: 't',
      symbol: sym,
      contract_type: contractType,
      barrier,
      req_id: ++reqId
    });
  }
  else if (msg.msg_type === 'proposal') {
    send({ buy: msg.proposal.id, price: msg.proposal.ask_price, req_id: ++reqId });
  }
  else if (msg.msg_type === 'buy') {
    const contractId = msg.buy.contract_id;
    const trade = state.activeTrades.find(t => !t.contractId);
    if (trade) {
      trade.contractId = contractId;
      addLog(`Bought: ${MARKETS[trade.market].name} – ID ${contractId}`);
      trade.timer = setTimeout(() => settleTrade(trade), 15000);
    }
  }
}

connectDeriv();
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
