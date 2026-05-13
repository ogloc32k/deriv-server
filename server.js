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

// ---------- Market definitions (with display names) ----------
const MARKETS = {
  "R_10":  { name: "Volatility 10 Index",  dp: 3 },
  "R_25":  { name: "Volatility 25 Index",  dp: 3 },
  "R_50":  { name: "Volatility 50 Index",  dp: 4 },
  "R_75":  { name: "Volatility 75 Index",  dp: 4 },
  "R_100": { name: "Volatility 100 Index", dp: 2 }
};

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
  logs: [],
  // store details for the pending trade to build the "Bought" log
  pendingTradeDetails: null
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
    state.currentStake = Math.max(state.stake, 0.35);
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
      state.stake = Math.max(parseFloat(stake), 0.35);
      if (!state.active) state.currentStake = state.stake;
    }
    if (martingale !== undefined) state.martingale = parseFloat(martingale);
    if (takeProfit !== undefined) state.takeProfit = parseFloat(takeProfit);
    if (stopLoss !== undefined) state.stopLoss = parseFloat(stopLoss);
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
    send({ ticks: state.marketSymbol, req_id: ++reqId });
  }
  else if (msg.msg_type === 'balance') {
    state.balance = msg.balance.balance;
    state.currency = msg.balance.currency;
    broadcastSSE({ state: sanitizeState() });
  }
  else if (msg.msg_type === 'tick') {
    const symbol = msg.tick.symbol;
    const price = msg.tick.quote;

    if (state.active && symbol === state.marketSymbol) {
      state.latestTick = price;
      const formatted = parseFloat(price).toFixed(state.dp);
      state.formattedPrice = formatted;
      state.lastDigit = formatted.slice(-1);

      // No extra log for outcome tick now – we’ll log the settlement directly

      if (!state.waitingForResult) {
        state.lastDigitsBuffer.push(state.lastDigit);
        if (state.lastDigitsBuffer.length > state.clusterSize) state.lastDigitsBuffer.shift();

        let tradeNow = false;
        if (state.triggerMode === 'single') {
          const triggerSet = state.triggerDigits.split(',').map(d => d.trim()).filter(d => d !== '');
          if (triggerSet.length === 0) {
            tradeNow = true;
          } else if (triggerSet.includes(state.lastDigit)) {
            tradeNow = true;
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
          const stakeToUse = Math.max(state.currentStake, 0.35);

          // Save details for the "Bought" log
          state.pendingTradeDetails = {
            market: state.marketSymbol,
            marketName: MARKETS[state.marketSymbol].name,
            direction: state.directionOverUnder,
            barrier: barrier,
            duration: 1,
            stake: stakeToUse
          };

          send({
            proposal: 1,
            amount: stakeToUse,
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
  }
  else if (msg.msg_type === 'proposal') {
    const proposalId = msg.proposal.id;
    const askPrice = msg.proposal.ask_price;
    // Buy immediately – no extra log
    send({ buy: proposalId, price: askPrice, req_id: ++reqId });
  }
  else if (msg.msg_type === 'buy') {
    const contractId = msg.buy.contract_id;
    state.pendingContractId = contractId;

    // Build and log the "Bought" line
    if (state.pendingTradeDetails) {
      const d = state.pendingTradeDetails;
      const directionWord = d.direction === 'over' ? 'higher' : 'lower';
      const boughtLine = `Bought: Win payout if the last digit of ${d.marketName} is strictly ${directionWord} than ${d.barrier} after ${d.duration} ticks. (ID: ${contractId})`;
      addLog(boughtLine);
    }

    send({ proposal_open_contract: 1, contract_id: contractId, subscribe: 1, req_id: ++reqId });
  }
  else if (msg.msg_type === 'proposal_open_contract') {
    const c = msg.proposal_open_contract;
    if (state.active && state.waitingForResult && c.contract_id === state.pendingContractId) {
      const netProfit = typeof c.profit === 'number' ? c.profit : parseFloat(c.profit) || 0;
      state.runningProfit += netProfit;

      // Timestamp for settlement
      const now = new Date();
      const dateStr = now.toISOString().replace('T', ' ').slice(0, 19) + ' GMT';
      addLog(dateStr);

      if (netProfit > 0) {
        addLog(`Profit amount: ${netProfit.toFixed(2)} USD`);
      } else if (netProfit < 0) {
        addLog(`Loss amount: ${netProfit.toFixed(2)} USD`);
      } else {
        addLog(`Draw: 0.00 USD`);
      }

      // Martingale
      if (netProfit < 0) {
        state.currentStake = Math.round(state.currentStake * state.martingale * 100) / 100;
        state.currentStake = Math.max(state.currentStake, 0.35);
      } else {
        state.currentStake = state.stake;
      }

      state.pendingContractId = null;
      state.waitingForResult = false;
      state.pendingTradeDetails = null;

      if (state.runningProfit >= state.takeProfit) {
        addLog('Take profit reached. Stopping.');
        state.active = false;
      } else if (state.runningProfit <= -state.stopLoss) {
        addLog('Stop loss hit. Stopping.');
        state.active = false;
      }
    }
    broadcastSSE({ state: sanitizeState() });
  }
}

// ---------- Initial connection ----------
connectDeriv();

server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
