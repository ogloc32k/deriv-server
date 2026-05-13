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
    this.shortTicks = [];
    this.shortCount = 0;
    this.shortMean = new Array(10).fill(0);
    this.shortM2 = new Array(10).fill(0);

    this.longTicks = [];
    this.longCount = 0;
    this.longMean = new Array(10).fill(0);
    this.longM2 = new Array(10).fill(0);

    this.prices = [];
    this.lastPrice = null;
  }

  feed(price, dp) {
    const digit = parseInt(parseFloat(price).toFixed(dp).slice(-1));
    // short
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
    // long
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
    // price
    this.prices.push(price);
    if (this.prices.length > 500) this.prices.shift();
    this.lastPrice = price;
  }

  getAnalysis() {
    if (this.shortCount < 5 || this.longCount < 2) return null;

    const shortRecent = this.shortTicks.slice(-100);
    const shortFreq = {};
    for (let d = 0; d < 10; d++) shortFreq[d] = shortRecent.filter(x => x === d).length / 100;

    const longRecent = this.longTicks.slice(-500);
    const longFreq = {};
    for (let d = 0; d < 10; d++) longFreq[d] = longRecent.filter(x => x === d).length / 500;

    const zShort = {}, zLong = {};
    for (let d = 0; d < 10; d++) {
      const vs = this.shortM2[d] / (this.shortCount - 1), ss = Math.sqrt(vs) || 0.01;
      zShort[d] = (shortFreq[d] - this.shortMean[d]) / ss;
      const vl = this.longM2[d] / (this.longCount - 1), sl = Math.sqrt(vl) || 0.01;
      zLong[d] = (longFreq[d] - this.longMean[d]) / sl;
    }

    let overConf = 0, underConf = 0;
    if (zShort[0] < 0 && zShort[1] < 0 && zLong[0] < 0 && zLong[1] < 0) {
      const rareShort = Math.min(3, Math.max(0, -Math.min(zShort[0], zShort[1]))) / 3;
      const rareLong = Math.min(3, Math.max(0, -Math.min(zLong[0], zLong[1]))) / 3;
      overConf = ((rareShort * 0.4 + rareLong * 0.6) + 0.4) * 100;
    }
    if (zShort[8] < 0 && zShort[9] < 0 && zLong[8] < 0 && zLong[9] < 0) {
      const rareShort = Math.min(3, Math.max(0, -Math.min(zShort[8], zShort[9]))) / 3;
      const rareLong = Math.min(3, Math.max(0, -Math.min(zLong[8], zLong[9]))) / 3;
      underConf = ((rareShort * 0.4 + rareLong * 0.6) + 0.4) * 100;
    }

    // price slope
    let priceSlope = 0;
    if (this.prices.length >= 20) {
      const recentPrices = this.prices.slice(-20).map(Number);
      const n = recentPrices.length;
      let sx = 0, sy = 0, sxy = 0, sx2 = 0;
      for (let i = 0; i < n; i++) {
        sx += i; sy += recentPrices[i]; sxy += i * recentPrices[i]; sx2 += i * i;
      }
      priceSlope = (n * sxy - sx * sy) / (n * sx2 - sx * sx);
    }

    const bestDirection = overConf >= underConf ? 'over' : 'under';
    const bestConf = Math.max(overConf, underConf);

    return {
      overConf, underConf, bestDirection, bestConf,
      priceSlope,
      shortFreq,   // current 100-tick frequencies (keys 0-9)
      longFreq,
      zShort, zLong
    };
  }
}

const analyzers = {};
for (const sym of Object.keys(MARKETS)) analyzers[sym] = new Analyzer();

// ---------- State ----------
const state = {
  active: false,
  balance: null,
  currency: 'USD',
  minConfidence: 45,
  riskPct: 1.5,
  dailyStopLossPct: 6,
  dailyTakeProfitPct: 20,
  maxConcurrent: 2,
  trendFilterEnabled: true,
  dailyStartBalance: null,
  dailyPnl: 0,
  locked: false,
  activeTrades: [],
  marketSignals: {},
  logs: []
};

// ---------- Helpers ----------
function dailyLimitReached() {
  if (!state.dailyStartBalance) return false;
  const pnlPct = (state.dailyPnl / state.dailyStartBalance) * 100;
  if (pnlPct <= -state.dailyStopLossPct) {
    addLog(`Daily stop loss reached (${pnlPct.toFixed(1)}%). Locked.`);
    return true;
  }
  if (pnlPct >= state.dailyTakeProfitPct) {
    addLog(`Daily profit target reached (${pnlPct.toFixed(1)}%). Locked.`);
    return true;
  }
  return false;
}

function calcStake() {
  if (!state.balance) return 0.35;
  return Math.max(0.35, Math.round((state.riskPct / 100) * state.balance * 100) / 100);
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

// ---------- Zone check ----------
function inZone(direction, price, dp) {
  // Last 2 digits for dp=2, last 3 digits for dp>=3
  const str = parseFloat(price).toFixed(dp);
  const decimalPart = str.includes('.') ? str.split('.')[1] : '';
  if (dp === 2) {
    const twoDigit = parseInt(decimalPart.slice(-2));
    if (isNaN(twoDigit)) return false;
    if (direction === 'over') return twoDigit <= 49;
    else return twoDigit >= 50;
  } else {
    const threeDigit = parseInt(decimalPart.slice(-3));
    if (isNaN(threeDigit)) return false;
    if (direction === 'over') return threeDigit <= 499;
    else return threeDigit >= 500;
  }
}

// ---------- EV + dynamic adjustment ----------
function expectedProfit(direction, barrier, shortFreq, analyzer) {
  const HOUSE = 0.98;
  let payout;
  if (direction === 'over') {
    payout = (10 / (9 - barrier)) * HOUSE;
  } else {
    payout = (10 / barrier) * HOUSE;
  }

  // base win probability from shortFreq
  let winProb = 0;
  if (direction === 'over') {
    for (let d = barrier + 1; d <= 9; d++) winProb += shortFreq[d] || 0;
  } else {
    for (let d = 0; d < barrier; d++) winProb += shortFreq[d] || 0;
  }

  // dynamic adjustment: digit momentum from last 3 single digits
  const lastDigits = analyzer.shortTicks.slice(-3);
  if (lastDigits.length === 3) {
    const diff1 = lastDigits[1] - lastDigits[0];
    const diff2 = lastDigits[2] - lastDigits[1];
    const trendDirection = (diff1 > 0 && diff2 > 0) ? 'up' : ((diff1 < 0 && diff2 < 0) ? 'down' : 'flat');
    if (direction === 'over' && trendDirection === 'up') winProb *= 1.05;
    else if (direction === 'over' && trendDirection === 'down') winProb *= 0.95;
    else if (direction === 'under' && trendDirection === 'up') winProb *= 0.95;
    else if (direction === 'under' && trendDirection === 'down') winProb *= 1.05;
  }

  const ev = winProb * (payout - 1) - (1 - winProb);
  return { winProb, payout, ev };
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
    state.active = true; state.locked = false;
    state.dailyStartBalance = state.balance; state.dailyPnl = 0; state.activeTrades = [];
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
let derivWs = null; let reqId = 0;
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
    for (const sym of Object.keys(MARKETS)) send({ ticks_history: sym, count: 1000, end: 'latest', req_id: ++reqId });
  }
  else if (msg.msg_type === 'balance') {
    state.balance = msg.balance.balance;
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'history') {
    const sym = msg.echo_req.ticks_history;
    const analyzer = analyzers[sym];
    if (analyzer && msg.history && msg.history.prices)
      for (const p of msg.history.prices) analyzer.feed(p, MARKETS[sym].dp);
    send({ ticks: sym, req_id: ++reqId });
  }
  else if (msg.msg_type === 'tick') {
    const sym = msg.tick.symbol;
    if (!MARKETS[sym]) return;
    const price = msg.tick.quote;
    const market = MARKETS[sym];
    const analyzer = analyzers[sym];
    analyzer.feed(price, market.dp);

    const analysis = analyzer.getAnalysis();
    if (analysis) {
      state.marketSignals[sym] = {
        over: analysis.overConf.toFixed(1),
        under: analysis.underConf.toFixed(1),
        trend: analysis.priceSlope > 0.0001 ? 'up' : (analysis.priceSlope < -0.0001 ? 'down' : 'flat'),
        bestDirection: analysis.bestDirection,
        bestConf: analysis.bestConf.toFixed(1)
      };
    }
    broadcastSSE({ state: sanitizeState() });

    if (!state.active || state.locked || !analysis || analysis.bestConf < state.minConfidence) return;

    // Zone check
    if (!inZone(analysis.bestDirection, price, market.dp)) return;

    // Trend filter
    if (state.trendFilterEnabled) {
      const slope = analysis.priceSlope;
      if (analysis.bestDirection === 'over' && slope < -0.0001) return;
      if (analysis.bestDirection === 'under' && slope > 0.0001) return;
    }

    // Optimal barrier
    const barrier = (() => {
      let best = null, bestEv = -Infinity;
      const maxOver = 4, minUnder = 6;
      if (analysis.bestDirection === 'over') {
        for (let n = 0; n <= maxOver; n++) {
          const { ev } = expectedProfit('over', n, analysis.shortFreq, analyzer);
          if (ev > bestEv) { bestEv = ev; best = n; }
        }
      } else {
        for (let n = minUnder; n <= 9; n++) {
          const { ev } = expectedProfit('under', n, analysis.shortFreq, analyzer);
          if (ev > bestEv) { bestEv = ev; best = n; }
        }
      }
      return bestEv > 0 ? best : null;
    })();

    if (barrier === null) return;

    if (state.activeTrades.some(t => t.market === sym)) return;
    if (dailyLimitReached()) { state.locked = true; return; }
    if (!canOpenTrade(sym)) return;

    const stake = calcStake();
    const direction = analysis.bestDirection;
    const contractType = direction === 'over' ? 'DIGITOVER' : 'DIGITUNDER';
    const trade = { market: sym, direction, barrier, stake, balanceBefore: state.balance, timer: null };
    state.activeTrades.push(trade);
    addLog(`${market.name} signal ${analysis.bestConf.toFixed(0)}% – ${contractType} barrier ${barrier}, stake ${stake.toFixed(2)}`);
    send({
      proposal: 1, amount: stake, basis: 'stake', currency: state.currency || 'USD',
      duration: 1, duration_unit: 't', symbol: sym, contract_type: contractType, barrier, req_id: ++reqId
    });
  }
  else if (msg.msg_type === 'proposal') {
    send({ buy: msg.proposal.id, price: msg.proposal.ask_price, req_id: ++reqId });
  }
  else if (msg.msg_type === 'buy') {
    const trade = state.activeTrades.find(t => !t.contractId);
    if (trade) {
      trade.contractId = msg.buy.contract_id;
      addLog(`Bought: ${MARKETS[trade.market].name} – ID ${msg.buy.contract_id}`);
      trade.timer = setTimeout(() => settleTrade(trade), 15000);
    }
  }
}

connectDeriv();
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
