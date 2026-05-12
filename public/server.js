const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

// ---------- static files ----------
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ---------- SSE clients ----------
const sseClients = new Set();
let logId = 1;

// ---------- Trading State ----------
const state = {
  active: false,
  market: 'R_100',
  digit: '5',                 // last digit trigger
  direction: 'CALL',
  barrierOffset: 1.0,
  duration: 1,               // ticks
  stake: 1.0,
  martingale: 2.0,
  takeProfit: 10.0,
  stopLoss: 10.0,
  balance: null,
  currency: 'USD',
  totalProfit: 0,
  currentStake: null,
  runningProfit: 0,          // accumulated profit/loss from trades
  pendingContractId: null,
  waitingForResult: false,
  tickCooldown: false,
  logs: []
};

// ---------- Helpers ----------
function addLog(msg) {
  const entry = { id: logId++, time: new Date().toISOString(), message: msg };
  state.logs.unshift(entry);
  if (state.logs.length > 200) state.logs.pop();
  sseClients.forEach(c => c.write(`data: ${JSON.stringify({ logs: [entry] })}\n\n`));
}

function broadcastState() {
  sseClients.forEach(c => c.write(`data: ${JSON.stringify({ state })}\n\n`));
}

// ---------- SSE endpoint (logs + state) ----------
app.get('/api/logs', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive'
  });
  res.write('\n');
  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

// ---------- REST endpoints ----------
app.get('/api/state', (req, res) => {
  res.json({ ...state, logs: undefined });
});

app.post('/api/control', (req, res) => {
  const { action } = req.body;
  if (action === 'start') {
    if (!process.env.DERIV_APP_ID || !process.env.DERIV_API_TOKEN) {
      return res.status(400).json({ error: 'Missing Deriv credentials' });
    }
    state.active = true;
    state.totalProfit = 0;
    state.runningProfit = 0;
    state.currentStake = state.stake;
    state.waitingForResult = false;
    state.tickCooldown = false;
    state.pendingContractId = null;
    connectDeriv();
    addLog('Trading started.');
    res.json({ success: true, state });
  } else if (action === 'stop') {
    state.active = false;
    if (derivWs) derivWs.close();
    addLog('Trading stopped.');
    res.json({ success: true, state });
  } else if (action === 'update') {
    const { market, digit, direction, barrierOffset, duration, stake, martingale, takeProfit, stopLoss } = req.body;
    if (market) state.market = market;
    if (digit !== undefined) state.digit = String(digit);
    if (direction) state.direction = direction;
    if (barrierOffset !== undefined) state.barrierOffset = parseFloat(barrierOffset);
    if (duration !== undefined) state.duration = parseInt(duration);
    if (stake !== undefined) { state.stake = parseFloat(stake); if (!state.active) state.currentStake = parseFloat(stake); }
    if (martingale !== undefined) state.martingale = parseFloat(martingale);
    if (takeProfit !== undefined) state.takeProfit = parseFloat(takeProfit);
    if (stopLoss !== undefined) state.stopLoss = parseFloat(stopLoss);
    res.json({ success: true, state });
  } else {
    res.status(400).json({ error: 'Invalid action' });
  }
});

// ---------- Deriv WebSocket ----------
let derivWs = null;
let reqId = 0;

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
    addLog('Deriv connection closed.');
    state.active = false;
    broadcastState();
  });

  derivWs.on('error', err => {
    addLog(`WebSocket error: ${err.message}`);
    state.active = false;
    broadcastState();
  });
}

function handleDerivMessage(msg) {
  if (msg.error) {
    addLog(`Deriv error: ${msg.error.code} - ${msg.error.message}`);
    return;
  }

  // Authorization response
  if (msg.msg_type === 'authorize') {
    addLog('Authorized. Requesting balance & ticks...');
    send({ balance: 1, subscribe: 1, req_id: ++reqId });
    send({ ticks: state.market, req_id: ++reqId });
    return;
  }

  // Balance update (or initial response)
  if (msg.msg_type === 'balance') {
    state.balance = msg.balance.balance;
    state.currency = msg.balance.currency;
    broadcastState();
    return;
  }

  // Tick stream
  if (msg.msg_type === 'tick') {
    if (!state.active || state.waitingForResult || state.tickCooldown) return;
    const price = msg.tick.quote;
    const lastChar = price.toString().slice(-1);
    if (lastChar === state.digit) {
      startTrade(price);
    }
    return;
  }

  // Proposal response → buy immediately
  if (msg.msg_type === 'proposal') {
    const proposalId = msg.proposal.id;
    const askPrice = msg.proposal.ask_price;
    addLog(`Proposal received. Buying contract ${proposalId} at price ${askPrice}`);
    send({ buy: proposalId, price: askPrice, req_id: ++reqId });
    return;
  }

  // Buy response → subscribe to open contract
  if (msg.msg_type === 'buy') {
    const contractId = msg.buy.contract_id;
    state.pendingContractId = contractId;
    addLog(`Contract ${contractId} opened. Subscribing to updates...`);
    send({ proposal_open_contract: 1, contract_id: contractId, subscribe: 1, req_id: ++reqId });
    return;
  }

  // Open contract update (including settlement)
  if (msg.msg_type === 'proposal_open_contract') {
    const c = msg.proposal_open_contract;
    if (c.is_sold && c.contract_id === state.pendingContractId) {
      const profit = parseFloat(c.profit) || 0;
      state.runningProfit += profit;
      state.totalProfit = state.runningProfit;
      addLog(`Trade ${c.contract_id} ended: ${profit >= 0 ? 'WIN' : 'LOSS'} | Profit: ${profit.toFixed(2)} | Total: ${state.runningProfit.toFixed(2)}`);

      if (profit < 0) {
        state.currentStake = Math.round(state.currentStake * state.martingale * 100) / 100;
      } else {
        state.currentStake = state.stake;
      }

      state.pendingContractId = null;
      state.waitingForResult = false;
      broadcastState();

      // Check TP / SL
      if (state.runningProfit >= state.takeProfit) {
        addLog(`Take Profit reached (${state.runningProfit.toFixed(2)}). Stopping.`);
        state.active = false;
        derivWs.close();
      } else if (state.runningProfit <= -state.stopLoss) {
        addLog(`Stop Loss hit (${state.runningProfit.toFixed(2)}). Stopping.`);
        state.active = false;
        derivWs.close();
      }
    }
    return;
  }
}

function startTrade(currentPrice) {
  if (!state.active || state.waitingForResult) return;

  state.waitingForResult = true;
  state.tickCooldown = true;
  setTimeout(() => { state.tickCooldown = false; }, 500);

  const barrier = (parseFloat(currentPrice) + (state.direction === 'CALL' ? state.barrierOffset : -state.barrierOffset)).toFixed(2);

  addLog(`Triggered! Last digit = ${state.digit}. Requesting proposal for ${state.direction} ${state.market} barrier=${barrier} stake=${state.currentStake}`);

  send({
    proposal: 1,
    amount: state.currentStake,
    basis: 'stake',
    contract_type: state.direction,
    currency: state.currency,
    duration: state.duration,
    duration_unit: 't',
    underlying_symbol: state.market,
    barrier: barrier,
    req_id: ++reqId
  });
}

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
