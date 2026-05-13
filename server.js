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

// ---------- Manual Trading State ----------
const state = {
  // manual mode fields
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
  // auto‑trade fields
  autoActive: false,
  autoAnalysis: null,       // current signal details
  autoSessionProfit: 0,
  autoWaitingForResult: false,
  autoPendingContractId: null,
  dailyPnL: 0,
  cooldownUntil: null,      // timestamp when cooldown ends
  logs: []
};

// ---------- AdaptiveDigitAnalyzer (JS version) ----------
class AdaptiveDigitAnalyzer {
  constructor(symbol, dp) {
    this.symbol = symbol;
    this.dp = dp;
    this.ticks = [];
    this.maxTicks = 1000;
    this.lastDigit = null;
    this.prevDigit = null;
    this.snapshotFreqs = [];
    this.maxSnapshots = 15;
    this.snapshotCounter = 0;
    this.kingHistory = [];
    this.kingFreqHistory = [];
  }

  updatePrice(price) {
    const digit = parseInt(parseFloat(price).toFixed(this.dp).slice(-1));
    this.prevDigit = this.lastDigit;
    this.lastDigit = digit;
    this.ticks.push(digit);
    if (this.ticks.length > this.maxTicks) this.ticks.shift();
    this.snapshotCounter++;
    if (this.snapshotCounter >= 100) {
      this._takeSnapshot();
      this.snapshotCounter = 0;
    }
  }

  feedHistory(prices) {
    for (const price of prices) {
      this.updatePrice(price);
    }
  }

  _takeSnapshot() {
    if (this.ticks.length < 100) return;
    const recent = this.ticks.slice(-100);
    const freq = {};
    for (let d = 0; d < 10; d++) {
      freq[d] = recent.filter(x => x === d).length / 100;
    }
    this.snapshotFreqs.push(freq);
    if (this.snapshotFreqs.length > this.maxSnapshots) this.snapshotFreqs.shift();
    const king = Object.keys(freq).reduce((a, b) => freq[a] > freq[b] ? a : b, '0');
    this.kingHistory.push(parseInt(king));
    this.kingFreqHistory.push(freq[king]);
    if (this.kingHistory.length > 5) {
      this.kingHistory.shift();
      this.kingFreqHistory.shift();
    }
  }

  getAnalysis() {
    if (this.snapshotFreqs.length < 5) return null;
    const means = {}, stds = {};
    for (let d = 0; d < 10; d++) {
      const vals = this.snapshotFreqs.map(snap => snap[d]);
      const mean = vals.reduce((a, b) => a + b, 0) / vals.length;
      means[d] = mean;
      const variance = vals.reduce((a, v) => a + (v - mean) ** 2, 0) / vals.length;
      stds[d] = Math.sqrt(variance) || 0.01;
    }
    const current = this.snapshotFreqs[this.snapshotFreqs.length - 1];
    const z = {};
    for (let d = 0; d < 10; d++) {
      z[d] = (current[d] - means[d]) / stds[d];
    }
    const king = Object.keys(z).reduce((a, b) => z[a] > z[b] ? a : b, '0');
    const slave = Object.keys(z).reduce((a, b) => z[a] < z[b] ? a : b, '0');
    let trendUp = false;
    if (this.kingFreqHistory.length >= 2) {
      const n = this.kingFreqHistory.length;
      let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
      for (let i = 0; i < n; i++) {
        sumX += i;
        sumY += this.kingFreqHistory[i];
        sumXY += i * this.kingFreqHistory[i];
        sumX2 += i * i;
      }
      const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
      trendUp = slope > 0.002;
    }
    let entropy = 0;
    for (const v of Object.values(current)) {
      if (v > 0) entropy -= v * Math.log(v);
    }
    const maxEntropy = Math.log(10);
    const normEntropy = Math.max(0, Math.min(1, 1 - entropy / maxEntropy));
    return {
      z,
      king: parseInt(king),
      slave: parseInt(slave),
      kingZ: z[king],
      slaveZ: z[slave],
      kingTrendUp: trendUp,
      entropy: normEntropy,
      lastDigit: this.lastDigit,
      prevDigit: this.prevDigit,
      currentFreq: current,
    };
  }
}

// Initialize analyzers for all markets
const analyzers = {};
for (const [sym, info] of Object.entries(MARKETS)) {
  analyzers[sym] = new AdaptiveDigitAnalyzer(sym, info.dp);
}

// ---------- Confidence Calculation (just like Python) ----------
function computeConfidence(analysis) {
  if (!analysis) return { over1: 0, under8: 0 };
  const z = analysis.z;
  const trend = analysis.kingTrendUp;
  let over1 = 0, under8 = 0;
  if (z[0] < 0 && z[1] < 0) {
    const rare = Math.min(3, Math.max(0, -Math.min(z[0], z[1]))) / 3;
    const kingStrength = Math.min(1, Math.max(0, analysis.kingZ / 3));
    const trendBonus = trend ? 0.25 : 0;
    over1 = (rare * 0.3 + kingStrength * 0.5 + trendBonus) * 100;
  }
  if (z[8] < 0 && z[9] < 0) {
    const rare = Math.min(3, Math.max(0, -Math.min(z[8], z[9]))) / 3;
    const kingWeak = Math.min(1, Math.max(0, -analysis.kingZ / 3));
    const trendBonus = trend ? 0.25 : 0;
    under8 = (rare * 0.3 + kingWeak * 0.5 + trendBonus) * 100;
  }
  return { over1, under8 };
}

// ---------- Auto‑Trader logic ----------
const AUTO_STAKE = 0.35;
const SESSION_PROFIT_TARGET = 0.40;
const DAILY_TP = 2.0;
const DAILY_SL = 2.0;
const COOLDOWN_MS = 2 * 60 * 60 * 1000; // 2 hours
const SIGNAL_INTERVAL = 600 * 1000; // 10 minutes

let currentAutoSignal = null;   // { market, direction, barrier, digits: [] }
let autoSessionProfit = 0;
let autoTradeInProgress = false;
let dailyPnL = 0;
let cooldownUntil = null;
let lastSignalScan = 0;

function resetAutoSession() {
  currentAutoSignal = null;
  autoSessionProfit = 0;
  autoTradeInProgress = false;
  state.autoSessionProfit = 0;
  state.autoWaitingForResult = false;
}

// Scan all markets and update currentAutoSignal if best
function scanForBestSignal() {
  let bestMarket = null;
  let bestConf = 0, bestDirection = null, bestBarrier = null, bestDigits = [];
  for (const [sym, analyzer] of Object.entries(analyzers)) {
    const analysis = analyzer.getAnalysis();
    if (!analysis) continue;
    const { over1, under8 } = computeConfidence(analysis);
    const maxConf = Math.max(over1, under8);
    if (maxConf > bestConf && maxConf >= 35 && maxConf <= 65) {
      bestConf = maxConf;
      bestMarket = sym;
      if (over1 > under8) {
        bestDirection = 'over';
        bestBarrier = 1;
        bestDigits = [0, 1];
      } else {
        bestDirection = 'under';
        bestBarrier = 8;
        bestDigits = [8, 9];
      }
    }
  }
  if (bestMarket) {
    currentAutoSignal = {
      market: bestMarket,
      direction: bestDirection,
      barrier: bestBarrier,
      digits: bestDigits,
      confidence: bestConf
    };
    addLog(`🤖 Auto signal: ${MARKETS[bestMarket].name} ${bestDirection==='over'?'DIGITOVER':'DIGITUNDER'} barrier ${bestBarrier}, confidence ${bestConf.toFixed(1)}%`);
    state.autoAnalysis = {
      market: MARKETS[bestMarket].name,
      direction: bestDirection === 'over' ? 'Over 1' : 'Under 8',
      confidence: bestConf.toFixed(1)
    };
  } else {
    currentAutoSignal = null;
    state.autoAnalysis = null;
    addLog(`🤖 No signal in 35-65% range`);
  }
  broadcastSSE({ state: sanitizeState() });
}

// Called every tick from any market
function autoTicker(symbol, price) {
  const analyzer = analyzers[symbol];
  if (!analyzer) return;
  analyzer.updatePrice(price);

  // Daily cooldown check
  const now = Date.now();
  if (cooldownUntil && now < cooldownUntil) return;
  if (cooldownUntil && now >= cooldownUntil) {
    // Cooldown over – reset daily
    dailyPnL = 0;
    cooldownUntil = null;
    addLog('🤖 Cooldown ended – daily P/L reset');
  }

  // Check daily limits
  if (dailyPnL >= DAILY_TP) {
    addLog(`🤖 Daily profit target $${DAILY_TP} reached – entering 2h cooldown`);
    cooldownUntil = now + COOLDOWN_MS;
    resetAutoSession();
    return;
  }
  if (dailyPnL <= -DAILY_SL) {
    addLog(`🤖 Daily stop loss $${DAILY_SL} hit – entering 2h cooldown`);
    cooldownUntil = now + COOLDOWN_MS;
    resetAutoSession();
    return;
  }

  // Scan signals periodically
  if (now - lastSignalScan >= SIGNAL_INTERVAL) {
    lastSignalScan = now;
    scanForBestSignal();
  }

  if (!state.autoActive) return;
  if (!currentAutoSignal || currentAutoSignal.market !== symbol) return;
  if (autoTradeInProgress) return;

  const digit = parseInt(parseFloat(price).toFixed(MARKETS[symbol].dp).slice(-1));
  if (!currentAutoSignal.digits.includes(digit)) return;

  // Place trade
  autoTradeInProgress = true;
  state.autoWaitingForResult = true;

  const contractType = currentAutoSignal.direction === 'over' ? 'DIGITOVER' : 'DIGITUNDER';
  addLog(`🤖 Placing ${contractType} barrier ${currentAutoSignal.barrier} on ${MARKETS[symbol].name}`);

  send({
    proposal: 1,
    amount: AUTO_STAKE,
    basis: 'stake',
    currency: state.currency || 'USD',
    duration: 1,
    duration_unit: 't',
    symbol: symbol,
    contract_type: contractType,
    barrier: currentAutoSignal.barrier,
    req_id: ++reqId
  });
}

// Handle contract settlement for auto‑trader
function handleAutoContractSettlement(contract) {
  const profit = parseFloat(contract.profit) || 0;
  dailyPnL += profit;
  state.dailyPnL = dailyPnL;

  if (profit < 0) {
    addLog(`🤖 Auto trade LOSS: ${profit.toFixed(2)}. Session ended.`);
    resetAutoSession();
  } else {
    autoSessionProfit += profit;
    state.autoSessionProfit = autoSessionProfit;
    if (autoSessionProfit >= SESSION_PROFIT_TARGET) {
      addLog(`🤖 Session profit target $${SESSION_PROFIT_TARGET} reached (total +${autoSessionProfit.toFixed(2)}).`);
      resetAutoSession();
    } else {
      addLog(`🤖 Auto trade WIN: ${profit.toFixed(2)} (session +${autoSessionProfit.toFixed(2)})`);
    }
  }
  autoTradeInProgress = false;
  state.autoWaitingForResult = false;
  broadcastSSE({ state: sanitizeState() });
}

// ---------- SSE endpoint ----------
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

// ---------- Manual trading control ----------
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

// ---------- Auto‑trade control ----------
app.post('/api/auto', (req, res) => {
  const { action } = req.body;
  if (action === 'start') {
    if (!process.env.DERIV_APP_ID || !process.env.DERIV_API_TOKEN) {
      return res.status(400).json({ error: 'Missing Deriv credentials' });
    }
    state.autoActive = true;
    resetAutoSession();
    addLog('🤖 Auto‑trading started');
  } else if (action === 'stop') {
    state.autoActive = false;
    resetAutoSession();
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
    // Request historical ticks for each market to prime analyzers
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

    // Always feed analyzer for auto‑trader
    if (analyzers[symbol]) {
      autoTicker(symbol, price);
    }

    // Manual trading logic (only if active and symbol matches)
    if (state.active && symbol === state.marketSymbol) {
      state.latestTick = price;
      const formatted = parseFloat(price).toFixed(state.dp);
      state.formattedPrice = formatted;
      state.lastDigit = formatted.slice(-1);

      if (!state.waitingForResult) {
        state.lastDigitsBuffer.push(state.lastDigit);
        if (state.lastDigitsBuffer.length > state.clusterSize) state.lastDigitsBuffer.shift();

        let tradeNow = false;
        let triggerReason = '';
        if (state.triggerMode === 'single') {
          const triggerSet = state.triggerDigits.split(',').map(d => d.trim()).filter(d => d !== '');
          if (triggerSet.length === 0) {
            tradeNow = true;
            triggerReason = 'all';
          } else if (triggerSet.includes(state.lastDigit)) {
            tradeNow = true;
            triggerReason = `single: ${state.lastDigit}`;
          }
        } else if (state.triggerMode === 'cluster') {
          if (state.lastDigitsBuffer.length === state.clusterSize) {
            const clusterSet = state.clusterDigits.split(',').map(d => d.trim()).filter(d => d !== '');
            if (clusterSet.length === 0) {
              tradeNow = true;
              triggerReason = 'cluster: any';
            } else {
              const allInSet = state.lastDigitsBuffer.every(d => clusterSet.includes(d));
              if (allInSet) {
                tradeNow = true;
                triggerReason = `cluster: [${state.lastDigitsBuffer.join(',')}]`;
              }
            }
          }
        }
        if (tradeNow) {
          // manual trade
          state.waitingForResult = true;
          const contractType = state.directionOverUnder === 'over' ? 'DIGITOVER' : 'DIGITUNDER';
          const barrier = parseInt(state.barrierDigit);
          addLog(`Manual: Placing ${contractType} barrier ${barrier} | trigger: ${triggerReason}`);
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
      // After feeding history, subscribe to live ticks
      send({ ticks: sym, req_id: ++reqId });
    }
  } else if (msg.msg_type === 'proposal') {
    // Handle both manual and auto by checking which one is waiting
    const proposalId = msg.proposal.id;
    const askPrice = msg.proposal.ask_price;
    addLog(`Proposal received. Buying at ${askPrice}`);
    send({ buy: proposalId, price: askPrice, req_id: ++reqId });
  } else if (msg.msg_type === 'buy') {
    const contractId = msg.buy.contract_id;
    if (state.active && state.waitingForResult && state.pendingContractId === null) {
      // Manual trade
      state.pendingContractId = contractId;
      addLog(`Manual contract ${contractId} opened.`);
    } else if (state.autoActive && autoTradeInProgress && state.autoWaitingForResult && state.autoPendingContractId === undefined) {
      state.autoPendingContractId = contractId;
      addLog(`🤖 Auto contract ${contractId} opened.`);
    }
    // Subscribe to updates for both
    send({ proposal_open_contract: 1, contract_id: contractId, subscribe: 1, req_id: ++reqId });
  } else if (msg.msg_type === 'proposal_open_contract') {
    const c = msg.proposal_open_contract;
    // Check if it belongs to manual or auto
    if (state.active && state.waitingForResult && c.contract_id === state.pendingContractId) {
      const profit = parseFloat(c.profit) || 0;
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
    } else if (state.autoActive && autoTradeInProgress && c.contract_id === state.autoPendingContractId) {
      handleAutoContractSettlement(c);
      state.autoPendingContractId = null;
    }
    broadcastSSE({ state: sanitizeState() });
  }
}

// ---------- Initial connection ----------
connectDeriv();

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
