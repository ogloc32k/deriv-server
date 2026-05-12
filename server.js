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
  broadcastState(); // send full state so logs appear immediately
  // Also broadcast the log entry separately so the UI can prepend it cleanly
  sseClients.forEach(c => c.write(`data: ${JSON.stringify({ logs: [entry] })}\n\n`));
}

function broadcastState() {
  // Send the **full** state (except the logs array) so the UI always has all dynamic fields
  const { logs, ...rest } = state;
  sseClients.forEach(c => c.write(`data: ${JSON.stringify({ state: rest })}\n\n`));
}

// ---------- Market definitions ----------
const MARKETS = {
  "R_10":  { name: "V10",  dp: 3 },
  "R_25":  { name: "V25",  dp: 3 },
  "R_50":  { name: "V50",  dp: 4 },
  "R_75":  { name: "V75",  dp: 4 },
  "R_100": { name: "V100", dp: 2 }
};

// ---------- Trading State ----------
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
  logs: []
};

// ---------- SSE endpoint ----------
app.get('/api/logs', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive'
  });
  res.write('\n');
  sseClients.add(res);
  // Send full current state when client connects
  const { logs, ...rest } = state;
  res.write(`data: ${JSON.stringify({ state: rest })}\n\n`);
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
    addLog('Trading started.');
    broadcastState();
    res.json({ success: true, state });
  }
  else if (action === 'stop') {
    state.active = false;
    addLog('Trading stopped.');
    broadcastState();
    res.json({ success: true, state });
  }
  else if (action === 'update') {
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

    broadcastState();
    res.json({ success: true, state });
  }
  else {
    res.status(400).json({ error: 'Invalid action' });
  }
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
    send({ ticks: state.marketSymbol, req_id: ++reqId });
  }
  else if (msg.msg_type === 'balance') {
    state.balance = msg.balance.balance;
    state.currency = msg.balance.currency;
    broadcastState();
  }
  else if (msg.msg_type === 'tick') {
    if (msg.tick.symbol !== state.marketSymbol) return;

    const rawPrice = msg.tick.quote;
    state.latestTick = rawPrice;
    const formatted = parseFloat(rawPrice).toFixed(state.dp);
    state.formattedPrice = formatted;
    state.lastDigit = formatted.slice(-1);
    broadcastState(); // ✅ now sends full state → tick appears in UI

    if (!state.active || state.waitingForResult) return;

    // Maintain rolling buffer for last N digits
    state.lastDigitsBuffer.push(state.lastDigit);
    if (state.lastDigitsBuffer.length > state.clusterSize) {
      state.lastDigitsBuffer.shift();
    }

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
    }
    else if (state.triggerMode === 'cluster') {
      if (state.lastDigitsBuffer.length === state.clusterSize) {
        const clusterSet = state.clusterDigits.split(',').map(d => d.trim()).filter(d => d !== '');
        if (clusterSet.length === 0) {
          tradeNow = true;
          triggerReason = 'cluster: any (empty set)';
        } else {
          const allInSet = state.lastDigitsBuffer.every(d => clusterSet.includes(d));
          if (allInSet) {
            tradeNow = true;
            triggerReason = `cluster: [${state.lastDigitsBuffer.join(',')}] ∈ {${clusterSet.join(',')}}`;
          }
        }
      }
    }

    if (tradeNow) startTrade(triggerReason);
  }
  else if (msg.msg_type === 'proposal') {
    const proposalId = msg.proposal.id;
    const askPrice = msg.proposal.ask_price;
    addLog(`Proposal received. Buying at ${askPrice}`);
    send({ buy: proposalId, price: askPrice, req_id: ++reqId });
  }
  else if (msg.msg_type === 'buy') {
    const contractId = msg.buy.contract_id;
    state.pendingContractId = contractId;
    addLog(`Contract ${contractId} opened.`);
    send({ proposal_open_contract: 1, contract_id: contractId, subscribe: 1, req_id: ++reqId });
  }
  else if (msg.msg_type === 'proposal_open_contract') {
    const c = msg.proposal_open_contract;
    if (c.is_sold && c.contract_id === state.pendingContractId) {
      const profit = parseFloat(c.profit) || 0;
      state.runningProfit += profit;
      addLog(`Settled: ${profit >= 0 ? 'WIN' : 'LOSS'} | Profit: ${profit.toFixed(2)} | Total: ${state.runningProfit.toFixed(2)}`);

      if (profit < 0) {
        state.currentStake = Math.round(state.currentStake * state.martingale * 100) / 100;
      } else {
        state.currentStake = state.stake;
      }

      state.pendingContractId = null;
      state.waitingForResult = false;
      broadcastState();

      if (state.runningProfit >= state.takeProfit) {
        addLog(`Take Profit reached (${state.runningProfit.toFixed(2)}). Stopping.`);
        state.active = false;
      } else if (state.runningProfit <= -state.stopLoss) {
        addLog(`Stop Loss hit (${state.runningProfit.toFixed(2)}). Stopping.`);
        state.active = false;
      }
    }
  }
}

function startTrade(reason) {
  if (!state.active || state.waitingForResult) return;

  state.waitingForResult = true;

  const contractType = state.directionOverUnder === 'over' ? 'DIGITOVER' : 'DIGITUNDER';
  const barrier = parseInt(state.barrierDigit);

  addLog(`Placing ${contractType} barrier ${barrier} | trigger: ${reason}`);

  send({
    proposal: 1,
    amount: state.currentStake,
    basis: 'stake',
    currency: state.currency,
    duration: 1,
    duration_unit: 't',
    symbol: state.marketSymbol,
    contract_type: contractType,
    barrier: barrier,
    req_id: ++reqId
  });
}

// ---------- Initial connection ----------
connectDeriv();

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
