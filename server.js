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

// ---------- Market definitions ----------
const MARKETS = {
  "R_10":  { name: "V10",  dp: 3 },
  "R_25":  { name: "V25",  dp: 3 },
  "R_50":  { name: "V50",  dp: 4 },
  "R_75":  { name: "V75",  dp: 4 },
  "R_100": { name: "V100", dp: 2 }
};

// ---------- AdaptiveDigitAnalyzer (multi‑timeframe + bias correction) ----------
class AdaptiveDigitAnalyzer {
  constructor(symbol, dp) {
    this.symbol = symbol;
    this.dp = dp;

    this.ticks = [];
    this.prices = [];
    this.maxTicks = 1000;
    this.lastDigit = null;
    this.prevDigit = null;
    this.snapshotCounter = 0;
    this.shortSnapshotFreqs = [];
    this.maxShortSnapshots = 15;
    this.shortStats = { mean: new Array(10).fill(0), m2: new Array(10).fill(0), count: 0 };

    this.longWindow = [];
    this.longCounts = new Array(10).fill(0);
    this.longSnapshotCounter = 0;
    this.longSnapshotFreqs = [];
    this.maxLongSnapshots = 5;
    this.longStats = { mean: new Array(10).fill(0), m2: new Array(10).fill(0), count: 0 };
  }

  updatePrice(price) {
    const digit = parseInt(parseFloat(price).toFixed(this.dp).slice(-1));
    this.prevDigit = this.lastDigit;
    this.lastDigit = digit;

    this.ticks.push(digit);
    this.prices.push(price);
    if (this.ticks.length > this.maxTicks) {
      this.ticks.shift();
      this.prices.shift();
    }

    this.snapshotCounter++;
    if (this.snapshotCounter >= 100) {
      this._takeShortSnapshot();
      this.snapshotCounter = 0;
    }

    this.longWindow.push(digit);
    this.longCounts[digit]++;
    if (this.longWindow.length > 500) {
      const removed = this.longWindow.shift();
      this.longCounts[removed]--;
    }
    this.longSnapshotCounter++;
    if (this.longSnapshotCounter >= 500 && this.longWindow.length >= 500) {
      this._takeLongSnapshot();
      this.longSnapshotCounter = 0;
    }
  }

  feedHistory(prices) {
    for (const price of prices) {
      this.updatePrice(price);
    }
  }

  _takeShortSnapshot() {
    if (this.ticks.length < 100) return;
    const recent = this.ticks.slice(-100);
    const freq = {};
    for (let d = 0; d < 10; d++) freq[d] = recent.filter(x => x === d).length / 100;
    this.shortSnapshotFreqs.push(freq);
    if (this.shortSnapshotFreqs.length > this.maxShortSnapshots) this.shortSnapshotFreqs.shift();
    const stats = this.shortStats;
    stats.count++;
    for (let d = 0; d < 10; d++) {
      const delta = freq[d] - stats.mean[d];
      stats.mean[d] += delta / stats.count;
      const delta2 = freq[d] - stats.mean[d];
      stats.m2[d] += delta * delta2;
    }
  }

  _takeLongSnapshot() {
    if (this.longWindow.length < 500) return;
    const freq = {};
    for (let d = 0; d < 10; d++) freq[d] = this.longCounts[d] / 500;
    this.longSnapshotFreqs.push(freq);
    if (this.longSnapshotFreqs.length > this.maxLongSnapshots) this.longSnapshotFreqs.shift();
    const stats = this.longStats;
    stats.count++;
    for (let d = 0; d < 10; d++) {
      const delta = freq[d] - stats.mean[d];
      stats.mean[d] += delta / stats.count;
      const delta2 = freq[d] - stats.mean[d];
      stats.m2[d] += delta * delta2;
    }
  }

  _getBaselineZ(freq, stats) {
    const z = {};
    for (let d = 0; d < 10; d++) {
      const mean = stats.mean[d];
      const variance = stats.count > 1 ? stats.m2[d] / (stats.count - 1) : 0.0001;
      const std = Math.sqrt(variance) || 0.01;
      z[d] = (freq[d] - mean) / std;
    }
    return z;
  }

  getAnalysis() {
    if (this.shortStats.count < 5 || this.longStats.count < 2) return null;

    const currentShortFreq = this.shortSnapshotFreqs.length > 0
      ? this.shortSnapshotFreqs[this.shortSnapshotFreqs.length - 1]
      : (() => {
          if (this.ticks.length < 100) return null;
          const recent = this.ticks.slice(-100);
          const f = {};
          for (let d = 0; d < 10; d++) f[d] = recent.filter(x => x === d).length / 100;
          return f;
        })();
    if (!currentShortFreq) return null;

    if (this.longWindow.length < 500) return null;
    const currentLongFreq = {};
    for (let d = 0; d < 10; d++) currentLongFreq[d] = this.longCounts[d] / 500;

    const z_short = this._getBaselineZ(currentShortFreq, this.shortStats);
    const z_long  = this._getBaselineZ(currentLongFreq,  this.longStats);

    const king = Object.keys(currentShortFreq).reduce((a, b) => currentShortFreq[a] > currentShortFreq[b] ? a : b, '0');
    const kingZ = z_short[king];

    let trendUp = false;
    if (this.shortSnapshotFreqs.length >= 2) {
      const n = this.shortSnapshotFreqs.length;
      const y = this.shortSnapshotFreqs.map(s => s[king]);
      let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
      for (let i = 0; i < n; i++) {
        sumX += i; sumY += y[i]; sumXY += i * y[i]; sumX2 += i * i;
      }
      const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
      trendUp = slope > 0.002;
    }

    let entropy = 0;
    for (const v of Object.values(currentShortFreq)) {
      if (v > 0) entropy -= v * Math.log(v);
    }
    const maxEntropy = Math.log(10);
    const normEntropy = Math.max(0, Math.min(1, 1 - entropy / maxEntropy));

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

    let volatility = 0;
    if (this.prices.length >= 10) {
      const returns = [];
      for (let i = 1; i < this.prices.length; i++) {
        returns.push((this.prices[i] - this.prices[i-1]) / this.prices[i-1]);
      }
      const meanRet = returns.reduce((a,b) => a + b, 0) / returns.length;
      const variance = returns.reduce((a,b) => a + (b - meanRet)**2, 0) / returns.length;
      volatility = Math.sqrt(variance);
    }

    return {
      z_short, z_long, king: parseInt(king), kingZ, kingTrendUp: trendUp,
      entropy: normEntropy, lastDigit: this.lastDigit, prevDigit: this.prevDigit,
      currentShortFreq, currentLongFreq, priceSlope, volatility,
    };
  }
}

const analyzers = {};
for (const [sym, info] of Object.entries(MARKETS)) {
  analyzers[sym] = new AdaptiveDigitAnalyzer(sym, info.dp);
}

// ---------- Auto‑Trader Settings ----------
const AUTO_SETTINGS = {
  stake: 0.35,
  sessionProfitTarget: 0.40,
  dailyTP: 2.0,
  dailySL: 2.0,
  cooldownMs: 2 * 60 * 60 * 1000,
  signalIntervalMs: 600 * 1000,
  maxConsecutiveLossesPerSession: 2,
  dynamicConfidenceEnabled: true,
  baseConfRange: [35, 65],
  highVolConfRange: [40, 60],
  lowVolConfRange: [30, 70],
  volatilityThresholdHigh: 1.5,
  volatilityThresholdLow: 0.5,
  trendFilterEnabled: true,
  clusterConfirmationEnabled: true,
  clusterSize: 2,
  maxConcurrentSessions: 2,
  trailingStopEnabled: true,
  trailingThreshold: 0.30,
  trailingLock: 0.15,
  volatilityFilterEnabled: true,
  volatilityFilterThreshold: 0.001,
  sessionTimeoutMs: 15 * 60 * 1000,
  maxOverBarrier: 4,     // over barriers 0‑4 only
  minUnderBarrier: 6,    // under barriers 6‑9 only
};

let globalAvgVolatility = 0.0001;

// ---------- Manual Trading State ----------
const state = {
  active: false,
  marketSymbol: 'R_100',
  dp: MARKETS['R_100'].dp,
  triggerMode: 'single',
  triggerDigits: '',
  clusterDigits: '',
  clusterSize: 2,
  lastDigitsBuffer: [],
  barrierDigit: '3',
  directionOverUnder: 'over',
  stake: 1.0,
  martingale: 2.0,
  takeProfit: 10.0,
  stopLoss: 10.0,
  balance: null,
  currency: 'USD',
  runningProfit: 0,
  currentStake: 1.0,
  pendingContractId: null,
  waitingForResult: false,
  latestTick: null,
  formattedPrice: '',
  lastDigit: '',
  autoActive: false,
  autoSessions: [],
  dailyPnL: 0,
  cooldownUntil: null,
  logs: []
};

// ---------- Dynamic barrier selection ----------
const HOUSE_EDGE_FACTOR = 0.98;

function getFairPayoutOver(barrier) {
  if (barrier >= 9) return 1;
  return (10 / (9 - barrier)) * HOUSE_EDGE_FACTOR;
}

function getFairPayoutUnder(barrier) {
  if (barrier <= 0) return 1;
  return (10 / barrier) * HOUSE_EDGE_FACTOR;
}

function selectOptimalBarrier(direction, freq) {
  let bestBarrier = null, bestProfit = -Infinity;

  if (direction === 'over') {
    for (let n = 0; n <= AUTO_SETTINGS.maxOverBarrier; n++) {
      let winProb = 0;
      for (let d = n + 1; d <= 9; d++) winProb += (freq[d] || 0);
      const profit = winProb * getFairPayoutOver(n) - 1;
      if (profit > bestProfit) { bestProfit = profit; bestBarrier = n; }
    }
  } else {
    for (let n = AUTO_SETTINGS.minUnderBarrier; n <= 9; n++) {
      let winProb = 0;
      for (let d = 0; d < n; d++) winProb += (freq[d] || 0);
      const profit = winProb * getFairPayoutUnder(n) - 1;
      if (profit > bestProfit) { bestProfit = profit; bestBarrier = n; }
    }
  }

  if (bestProfit <= 0) return null;
  return { barrier: bestBarrier, expectedProfit: bestProfit };
}

// ---------- Confidence calculator ----------
function computeConfidence(analysis, dynamicRange) {
  if (!analysis) return { over1: 0, under8: 0 };
  const z = analysis.z_short;
  const trend = analysis.kingTrendUp;
  let over1 = 0, under8 = 0;
  if (z[0] < 0 && z[1] < 0 && analysis.z_long[0] < 0 && analysis.z_long[1] < 0) {
    const rare = Math.min(3, Math.max(0, -Math.min(z[0], z[1]))) / 3;
    const kingStrength = Math.min(1, Math.max(0, analysis.kingZ / 3));
    const trendBonus = trend ? 0.25 : 0;
    over1 = (rare * 0.3 + kingStrength * 0.5 + trendBonus) * 100;
  }
  if (z[8] < 0 && z[9] < 0 && analysis.z_long[8] < 0 && analysis.z_long[9] < 0) {
    const rare = Math.min(3, Math.max(0, -Math.min(z[8], z[9]))) / 3;
    const kingWeak = Math.min(1, Math.max(0, -analysis.kingZ / 3));
    const trendBonus = trend ? 0.25 : 0;
    under8 = (rare * 0.3 + kingWeak * 0.5 + trendBonus) * 100;
  }
  if (dynamicRange) {
    const [lo, hi] = dynamicRange;
    if (over1 < lo || over1 > hi) over1 = 0;
    if (under8 < lo || under8 > hi) under8 = 0;
  }
  return { over1, under8 };
}

function getDynamicConfRange(volatility) {
  if (!AUTO_SETTINGS.dynamicConfidenceEnabled) return AUTO_SETTINGS.baseConfRange;
  const ratio = volatility / globalAvgVolatility;
  if (ratio > AUTO_SETTINGS.volatilityThresholdHigh) return AUTO_SETTINGS.highVolConfRange;
  if (ratio < AUTO_SETTINGS.volatilityThresholdLow) return AUTO_SETTINGS.lowVolConfRange;
  return AUTO_SETTINGS.baseConfRange;
}

// ---------- Session management ----------
function createSession(signal) {
  return {
    market: signal.market,
    direction: signal.direction,
    barrier: signal.barrier,
    digits: signal.digits,
    sessionProfit: 0,
    peakProfit: 0,
    consecutiveLosses: 0,
    stopLevel: null,
    waitingForResult: false,
    pendingContractId: null,
    lastDigits: [],
    createdAt: Date.now(),
    lastTradeTime: null,
  };
}

function endSession(session, reason) {
  const idx = state.autoSessions.indexOf(session);
  if (idx > -1) state.autoSessions.splice(idx, 1);
  for (const [reqId, sess] of pendingAutoReqs) {
    if (sess === session) pendingAutoReqs.delete(reqId);
  }
  addLog(`🤖 Session ended (${reason}) | market ${MARKETS[session.market].name} | profit: ${session.sessionProfit.toFixed(2)}`);
  broadcastSSE({ state: sanitizeState() });
}

const pendingAutoReqs = new Map();

// ---------- Signal scanning (with dynamic barrier) ----------
let lastSignalScan = 0;

function scanForSignals() {
  const now = Date.now();
  if (now - lastSignalScan < AUTO_SETTINGS.signalIntervalMs) return;
  lastSignalScan = now;

  let totalVol = 0, count = 0;
  for (const sym of Object.keys(MARKETS)) {
    const a = analyzers[sym];
    const analysis = a.getAnalysis();
    if (analysis) { totalVol += analysis.volatility; count++; }
  }
  if (count > 0) globalAvgVolatility = totalVol / count;

  // Remove timed‑out sessions
  const now2 = Date.now();
  for (let i = state.autoSessions.length - 1; i >= 0; i--) {
    const session = state.autoSessions[i];
    const lastActivity = session.lastTradeTime || session.createdAt;
    if (now2 - lastActivity >= AUTO_SETTINGS.sessionTimeoutMs) {
      endSession(session, 'timeout');
    }
  }

  const candidates = [];
  for (const [sym, analyzer] of Object.entries(analyzers)) {
    const analysis = analyzer.getAnalysis();
    if (!analysis) continue;
    if (AUTO_SETTINGS.volatilityFilterEnabled && analysis.volatility > AUTO_SETTINGS.volatilityFilterThreshold) continue;

    const dynamicRange = getDynamicConfRange(analysis.volatility);
    const { over1, under8 } = computeConfidence(analysis, dynamicRange);

    const slope = analysis.priceSlope;
    const trendUp = slope > 0;
    const trendDown = slope < 0;
    const over1Allowed = !AUTO_SETTINGS.trendFilterEnabled || trendUp || Math.abs(slope) < 0.0001;
    const under8Allowed = !AUTO_SETTINGS.trendFilterEnabled || trendDown || Math.abs(slope) < 0.0001;

    if (over1 > 0 && over1Allowed) {
      const optimal = selectOptimalBarrier('over', analysis.currentShortFreq);
      if (optimal) {
        candidates.push({
          market: sym,
          direction: 'over',
          barrier: optimal.barrier,
          digits: [0, 1],
          confidence: over1,
        });
      }
    }
    if (under8 > 0 && under8Allowed) {
      const optimal = selectOptimalBarrier('under', analysis.currentShortFreq);
      if (optimal) {
        candidates.push({
          market: sym,
          direction: 'under',
          barrier: optimal.barrier,
          digits: [8, 9],
          confidence: under8,
        });
      }
    }
  }

  candidates.sort((a, b) => b.confidence - a.confidence);

  const existingMarkets = new Set(state.autoSessions.map(s => s.market));
  for (const signal of candidates) {
    if (state.autoSessions.length >= AUTO_SETTINGS.maxConcurrentSessions) break;
    if (existingMarkets.has(signal.market)) continue;
    const session = createSession(signal);
    state.autoSessions.push(session);
    addLog(`🤖 New session: ${MARKETS[signal.market].name} ${signal.direction==='over'?'DIGITOVER':'DIGITUNDER'} barrier ${signal.barrier} (edge ${signal.confidence.toFixed(1)}%)`);
  }
  broadcastSSE({ state: sanitizeState() });
}

// ---------- Auto ticker ----------
function autoTicker(symbol, price) {
  const analyzer = analyzers[symbol];
  if (!analyzer) return;
  analyzer.updatePrice(price);

  const now = Date.now();

  if (state.cooldownUntil && now < state.cooldownUntil) return;
  if (state.cooldownUntil && now >= state.cooldownUntil) {
    state.dailyPnL = 0;
    state.cooldownUntil = null;
    addLog('🤖 Cooldown ended – daily P/L reset');
    broadcastSSE({ state: sanitizeState() });
  }

  if (state.dailyPnL >= AUTO_SETTINGS.dailyTP) {
    addLog(`🤖 Daily profit target $${AUTO_SETTINGS.dailyTP} reached – cooldown 2h`);
    state.cooldownUntil = now + AUTO_SETTINGS.cooldownMs;
    while (state.autoSessions.length) endSession(state.autoSessions[0], 'daily TP');
    return;
  }
  if (state.dailyPnL <= -AUTO_SETTINGS.dailySL) {
    addLog(`🤖 Daily stop loss $${AUTO_SETTINGS.dailySL} hit – cooldown 2h`);
    state.cooldownUntil = now + AUTO_SETTINGS.cooldownMs;
    while (state.autoSessions.length) endSession(state.autoSessions[0], 'daily SL');
    return;
  }

  if (!state.autoActive) return;

  scanForSignals();

  const digit = parseInt(parseFloat(price).toFixed(MARKETS[symbol].dp).slice(-1));
  for (const session of state.autoSessions) {
    if (session.market !== symbol) continue;
    if (session.waitingForResult) continue;

    session.lastDigits.push(digit);
    if (session.lastDigits.length > AUTO_SETTINGS.clusterSize) session.lastDigits.shift();

    if (!session.digits.includes(digit)) continue;

    if (AUTO_SETTINGS.clusterConfirmationEnabled) {
      if (session.lastDigits.length < AUTO_SETTINGS.clusterSize) continue;
      const allMatch = session.lastDigits.every(d => session.digits.includes(d));
      if (!allMatch) continue;
    }

    session.waitingForResult = true;
    const contractType = session.direction === 'over' ? 'DIGITOVER' : 'DIGITUNDER';
    addLog(`🤖 Placing ${contractType} barrier ${session.barrier} on ${MARKETS[symbol].name}`);

    const currentReqId = ++reqId;
    send({
      proposal: 1,
      amount: AUTO_SETTINGS.stake,
      basis: 'stake',
      currency: state.currency || 'USD',
      duration: 1,
      duration_unit: 't',
      symbol: symbol,
      contract_type: contractType,
      barrier: session.barrier,
      req_id: currentReqId,
    });

    pendingAutoReqs.set(currentReqId, session);
    session.lastTradeTime = now;
  }
}

// ---------- Contract settlement (corrected win/loss logging) ----------
function handleAutoContractSettlement(contract, session) {
  const rawProfit = contract.profit;
  const profit = (typeof rawProfit === 'number') ? rawProfit : parseFloat(rawProfit) || 0;

  state.dailyPnL += profit;
  session.sessionProfit += profit;

  const resultText = profit >= 0 ? 'WIN' : 'LOSS';
  addLog(`🤖 Auto trade ${resultText}: ${profit.toFixed(2)} | Session profit: ${session.sessionProfit.toFixed(2)}`);

  if (profit < 0) {
    session.consecutiveLosses++;
  } else {
    session.consecutiveLosses = 0;
    if (session.sessionProfit > session.peakProfit) {
      session.peakProfit = session.sessionProfit;
      if (AUTO_SETTINGS.trailingStopEnabled && session.peakProfit >= AUTO_SETTINGS.trailingThreshold) {
        session.stopLevel = session.peakProfit - AUTO_SETTINGS.trailingLock;
        addLog(`🤖 Trailing stop set at $${session.stopLevel.toFixed(2)}`);
      }
    }
  }

  if (session.consecutiveLosses >= AUTO_SETTINGS.maxConsecutiveLossesPerSession) {
    endSession(session, 'max consecutive losses');
  } else if (session.sessionProfit >= AUTO_SETTINGS.sessionProfitTarget && !AUTO_SETTINGS.trailingStopEnabled) {
    endSession(session, 'session profit target');
  } else if (AUTO_SETTINGS.trailingStopEnabled && session.stopLevel !== null && session.sessionProfit <= session.stopLevel) {
    endSession(session, 'trailing stop');
  } else {
    session.waitingForResult = false;
    session.pendingContractId = null;
  }
  broadcastSSE({ state: sanitizeState() });
}

// ---------- SSE / API endpoints ----------
app.get('/api/logs', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive'
  });
  res.write('\n');
  sseClients.add(res);
  res.write(`data: ${JSON.stringify({ state: sanitizeState() })}\n\n`);
  req.on('close', () => sseClients.delete(res));
});

app.get('/api/state', (req, res) => {
  res.json({ ...state, logs: undefined });
});

app.post('/api/control', (req, res) => {
  const { action } = req.body;
  if (action === 'start') {
    state.active = true;
    state.runningProfit = 0;
    state.currentStake = state.stake;
    state.waitingForResult = false;
    state.pendingContractId = null;
    state.lastDigitsBuffer = [];
    addLog('Manual trading started.');
  } else if (action === 'stop') {
    state.active = false;
    addLog('Manual trading stopped.');
  } else if (action === 'update') {
    const {
      marketSymbol, triggerMode, triggerDigits,
      clusterDigits, clusterSize,
      barrierDigit, directionOverUnder,
      stake, martingale, takeProfit, stopLoss
    } = req.body;
    if (marketSymbol && MARKETS[marketSymbol]) {
      state.marketSymbol = marketSymbol;
      state.dp = MARKETS[marketSymbol].dp;
      state.lastDigitsBuffer = [];
      if (derivWs && derivWs.readyState === WebSocket.OPEN) {
        send({ ticks: state.marketSymbol, req_id: ++reqId });
      }
    }
    if (triggerMode) state.triggerMode = triggerMode;
    if (triggerDigits !== undefined) state.triggerDigits = String(triggerDigits).replace(/\s/g, '');
    if (clusterDigits !== undefined) state.clusterDigits = String(clusterDigits).replace(/\s/g, '');
    if (clusterSize !== undefined) state.clusterSize = parseInt(clusterSize) || 2;
    if (barrierDigit !== undefined) state.barrierDigit = String(barrierDigit);
    if (directionOverUnder) state.directionOverUnder = directionOverUnder;
    if (stake !== undefined) {
      state.stake = parseFloat(stake);
      if (!state.active) state.currentStake = parseFloat(stake);
    }
    if (martingale !== undefined) state.martingale = parseFloat(martingale);
    if (takeProfit !== undefined) state.takeProfit = parseFloat(takeProfit);
    if (stopLoss !== undefined) state.stopLoss = parseFloat(stopLoss);
  }
  broadcastSSE({ state: sanitizeState() });
  res.json({ success: true });
});

app.post('/api/auto', (req, res) => {
  const { action } = req.body;
  if (action === 'start') {
    if (!process.env.DERIV_APP_ID || !process.env.DERIV_API_TOKEN) {
      return res.status(400).json({ error: 'Missing Deriv credentials' });
    }
    state.autoActive = true;
    state.autoSessions = [];
    addLog('🤖 Auto‑trading started (dynamic barrier)');
  } else if (action === 'stop') {
    state.autoActive = false;
    state.autoSessions = [];
    addLog('🤖 Auto‑trading stopped');
  }
  broadcastSSE({ state: sanitizeState() });
  res.json({ success: true });
});

// ---------- Deriv WebSocket ----------
let derivWs = null;
let reqId = 0;
let reconnectTimer = null;

function send(msg) {
  if (derivWs && derivWs.readyState === WebSocket.OPEN) {
    derivWs.send(JSON.stringify(msg));
  }
}

function connectDeriv() {
  if (derivWs) derivWs.close();
  const appId = process.env.DERIV_APP_ID;
  derivWs = new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${appId}`);

  derivWs.on('open', () => {
    addLog('Connected to Deriv. Authorizing...');
    send({ authorize: process.env.DERIV_API_TOKEN });
  });

  derivWs.on('message', data => {
    try {
      handleDerivMessage(JSON.parse(data));
    } catch (e) {
      console.error('Invalid Deriv message', data);
    }
  });

  derivWs.on('close', () => {
    addLog('Deriv connection lost – reconnecting in 5s...');
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectDeriv, 5000);
  });

  derivWs.on('error', err => addLog(`WebSocket error: ${err.message}`));
}

function handleDerivMessage(msg) {
  if (msg.error) {
    addLog(`Deriv error: ${msg.error.code} - ${msg.error.message}`);
    return;
  }

  if (msg.msg_type === 'authorize') {
    addLog('Authorized. Subscribing to balance & ticks.');
    send({ balance: 1, subscribe: 1, req_id: ++reqId });
    for (const sym of Object.keys(MARKETS)) {
      send({ ticks_history: sym, count: 1000, end: 'latest', req_id: ++reqId });
    }
  } else if (msg.msg_type === 'balance') {
    state.balance = msg.balance.balance;
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  } else if (msg.msg_type === 'tick') {
    const symbol = msg.tick.symbol;
    const price = msg.tick.quote;

    if (analyzers[symbol]) autoTicker(symbol, price);

    if (state.active && symbol === state.marketSymbol) {
      state.latestTick = price;
      const formatted = parseFloat(price).toFixed(state.dp);
      state.formattedPrice = formatted;
      state.lastDigit = formatted.slice(-1);

      if (!state.waitingForResult) {
        state.lastDigitsBuffer.push(state.lastDigit);
        if (state.lastDigitsBuffer.length > state.clusterSize) state.lastDigitsBuffer.shift();

        let tradeNow = false, triggerReason = '';
        if (state.triggerMode === 'single') {
          const triggerSet = state.triggerDigits.split(',').map(d => d.trim()).filter(d => d !== '');
          if (triggerSet.length === 0) {
            tradeNow = true; triggerReason = 'all';
          } else if (triggerSet.includes(state.lastDigit)) {
            tradeNow = true; triggerReason = `single: ${state.lastDigit}`;
          }
        } else if (state.triggerMode === 'cluster') {
          if (state.lastDigitsBuffer.length === state.clusterSize) {
            const clusterSet = state.clusterDigits.split(',').map(d => d.trim()).filter(d => d !== '');
            if (clusterSet.length === 0 || state.lastDigitsBuffer.every(d => clusterSet.includes(d))) {
              tradeNow = true;
            }
          }
        }
        if (tradeNow) {
          state.waitingForResult = true;
          const contractType = state.directionOverUnder === 'over' ? 'DIGITOVER' : 'DIGITUNDER';
          const barrier = parseInt(state.barrierDigit);
          addLog(`Manual: Placing ${contractType} barrier ${barrier} | trigger: ${triggerReason || 'cluster'}`);
          send({
            proposal: 1,
            amount: state.currentStake,
            basis: 'stake',
            currency: state.currency || 'USD',
            duration: 1,
            duration_unit: 't',
            symbol: state.marketSymbol,
            contract_type: contractType,
            barrier: barrier,
            req_id: ++reqId
          });
        }
      }
    }
    broadcastSSE({ state: sanitizeState() });
  } else if (msg.msg_type === 'history') {
    const sym = msg.echo_req.ticks_history;
    const analyzer = analyzers[sym];
    if (analyzer && msg.history && msg.history.prices) {
      analyzer.feedHistory(msg.history.prices);
      send({ ticks: sym, req_id: ++reqId });
    }
  } else if (msg.msg_type === 'proposal') {
    const proposalId = msg.proposal.id;
    const askPrice = msg.proposal.ask_price;
    addLog(`Proposal received. Buying at ${askPrice}`);
    send({ buy: proposalId, price: askPrice, req_id: ++reqId });
  } else if (msg.msg_type === 'buy') {
    const contractId = msg.buy.contract_id;
    if (state.active && state.waitingForResult && !state.pendingContractId) {
      state.pendingContractId = contractId;
      addLog(`Manual contract ${contractId} opened.`);
    } else {
      let found = false;
      for (const [reqId, session] of pendingAutoReqs) {
        if (session.waitingForResult && !session.pendingContractId) {
          session.pendingContractId = contractId;
          pendingAutoReqs.delete(reqId);
          addLog(`🤖 Auto contract ${contractId} opened.`);
          found = true;
          break;
        }
      }
      if (!found) {
        for (const session of state.autoSessions) {
          if (session.waitingForResult && !session.pendingContractId) {
            session.pendingContractId = contractId;
            addLog(`🤖 Auto contract ${contractId} opened (fallback).`);
            break;
          }
        }
      }
    }
    send({ proposal_open_contract: 1, contract_id: contractId, subscribe: 1, req_id: ++reqId });
  } else if (msg.msg_type === 'proposal_open_contract') {
    const c = msg.proposal_open_contract;
    // Manual settlement
    if (state.active && state.waitingForResult && c.contract_id === state.pendingContractId) {
      const rawProfit = c.profit;
      const profit = (typeof rawProfit === 'number') ? rawProfit : parseFloat(rawProfit) || 0;
      state.runningProfit += profit;
      addLog(`Manual settled: ${profit >= 0 ? 'WIN' : 'LOSS'} | Profit: ${profit.toFixed(2)}`);
      if (profit < 0) {
        state.currentStake = Math.round(state.currentStake * state.martingale * 100) / 100;
      } else {
        state.currentStake = state.stake;
      }
      state.pendingContractId = null;
      state.waitingForResult = false;
      if (state.runningProfit >= state.takeProfit) {
        addLog(`Manual take profit reached. Stopping.`);
        state.active = false;
      } else if (state.runningProfit <= -state.stopLoss) {
        addLog(`Manual stop loss hit. Stopping.`);
        state.active = false;
      }
    } else {
      // Auto settlement
      for (const session of state.autoSessions) {
        if (session.waitingForResult && c.contract_id === session.pendingContractId) {
          handleAutoContractSettlement(c, session);
          break;
        }
      }
    }
    broadcastSSE({ state: sanitizeState() });
  }
}

// ---------- Initial connection ----------
connectDeriv();

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
