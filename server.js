const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

// Serve static files from public
app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// -------------------- Trading State --------------------
let tradingState = {
  active: false,
  market: 'R_100',
  digit: '5',
  direction: 'CALL',
  barrierOffset: 1.0,
  duration: 1,
  stake: 1.0,
  martingale: 1.0,
  takeProfit: 10,
  stopLoss: 10,
  totalProfit: 0,
  currentStake: 1.0,
  pendingContractId: null,
  waitingForResult: false,
  logs: [],
  tickCooldown: false
};

let derivWs = null;
let logId = 1;

// Helper to add log and send to all SSE clients
const sseClients = new Set();

function addLog(message) {
  const entry = { id: logId++, time: new Date().toISOString(), message };
  tradingState.logs.unshift(entry); // newest first
  if (tradingState.logs.length > 200) tradingState.logs.pop();
  // Notify SSE clients
  sseClients.forEach(client => {
    client.write(`data: ${JSON.stringify({ logs: [entry] })}\n\n`);
  });
}

// SSE endpoint for logs
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

// Control endpoint: Start/Stop
app.post('/api/control', (req, res) => {
  const { action } = req.body;
  if (action === 'start') {
    if (!process.env.DERIV_APP_ID || !process.env.DERIV_API_TOKEN) {
      return res.status(400).json({ error: 'Missing Deriv credentials in environment.' });
    }
    tradingState.totalProfit = 0;
    tradingState.currentStake = tradingState.stake;
    tradingState.pendingContractId = null;
    tradingState.waitingForResult = false;
    tradingState.tickCooldown = false;
    tradingState.active = true;
    connectDeriv();
    addLog('Trading started.');
    res.json({ success: true, state: tradingState });
  } else if (action === 'stop') {
    tradingState.active = false;
    if (derivWs) derivWs.close();
    addLog('Trading stopped.');
    res.json({ success: true, state: tradingState });
  } else if (action === 'update') {
    // Update parameters
    const { market, digit, direction, barrierOffset, duration, stake, martingale, takeProfit, stopLoss } = req.body;
    if (market) tradingState.market = market;
    if (digit !== undefined) tradingState.digit = String(digit);
    if (direction) tradingState.direction = direction;
    if (barrierOffset !== undefined) tradingState.barrierOffset = parseFloat(barrierOffset);
    if (duration !== undefined) tradingState.duration = parseInt(duration);
    if (stake !== undefined) { tradingState.stake = parseFloat(stake); if (!tradingState.active) tradingState.currentStake = parseFloat(stake); }
    if (martingale !== undefined) tradingState.martingale = parseFloat(martingale);
    if (takeProfit !== undefined) tradingState.takeProfit = parseFloat(takeProfit);
    if (stopLoss !== undefined) tradingState.stopLoss = parseFloat(stopLoss);
    res.json({ success: true, state: tradingState });
  } else {
    res.status(400).json({ error: 'Invalid action' });
  }
});

// Get current state
app.get('/api/state', (req, res) => {
  res.json({ ...tradingState, logs: undefined }); // logs are streamed
});

// -------------------- Deriv WebSocket Logic --------------------
function connectDeriv() {
  if (derivWs) derivWs.close();
  const appId = process.env.DERIV_APP_ID;
  derivWs = new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${appId}`);

  derivWs.on('open', () => {
    addLog('Connected to Deriv API.');
    // Authorize
    derivWs.send(JSON.stringify({ authorize: process.env.DERIV_API_TOKEN }));
  });

  derivWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      handleDerivMessage(msg);
    } catch (e) {
      console.error('Invalid Deriv message', data);
    }
  });

  derivWs.on('close', () => {
    addLog('Deriv WebSocket closed.');
    tradingState.active = false;
  });

  derivWs.on('error', (err) => {
    addLog(`Deriv WebSocket error: ${err.message}`);
    tradingState.active = false;
  });
}

function handleDerivMessage(msg) {
  if (msg.error) {
    addLog(`Deriv error: ${msg.error.code} - ${msg.error.message}`);
    return;
  }

  // After authorization, subscribe to ticks
  if (msg.msg_type === 'authorize') {
    addLog('Authorized. Subscribing to market ticks...');
    derivWs.send(JSON.stringify({ ticks: tradingState.market }));
  }

  // Tick stream
  if (msg.msg_type === 'tick') {
    if (!tradingState.active) return;
    const price = msg.tick.quote;
    const lastChar = price.toString().slice(-1);
    if (lastChar === tradingState.digit && !tradingState.waitingForResult && !tradingState.tickCooldown) {
      placeTrade(price);
    }
  }

  // Proposal open contract updates (after buying)
  if (msg.msg_type === 'proposal_open_contract') {
    const contract = msg.proposal_open_contract;
    if (contract.is_sold && contract.contract_id === tradingState.pendingContractId) {
      const profit = parseFloat(contract.profit);
      tradingState.totalProfit += profit;
      addLog(`Trade ${contract.contract_id} settled: ${profit >= 0 ? 'WIN' : 'LOSS'} | Profit: ${profit} | Total: ${tradingState.totalProfit.toFixed(2)}`);

      // Martingale logic
      if (profit < 0) {
        tradingState.currentStake *= tradingState.martingale;
        tradingState.currentStake = Math.round(tradingState.currentStake * 100) / 100; // round
      } else {
        tradingState.currentStake = tradingState.stake;
      }

      tradingState.pendingContractId = null;
      tradingState.waitingForResult = false;

      // Check Take Profit / Stop Loss
      if (tradingState.totalProfit >= tradingState.takeProfit) {
        addLog(`Take Profit reached (${tradingState.totalProfit.toFixed(2)}). Stopping.`);
        tradingState.active = false;
        derivWs.close();
      } else if (tradingState.totalProfit <= -tradingState.stopLoss) {
        addLog(`Stop Loss hit (${tradingState.totalProfit.toFixed(2)}). Stopping.`);
        tradingState.active = false;
        derivWs.close();
      }
    }
  }
}

function placeTrade(currentPrice) {
  if (!tradingState.active || tradingState.waitingForResult) return;

  const barrier = parseFloat(currentPrice) + (tradingState.direction === 'CALL' ? tradingState.barrierOffset : -tradingState.barrierOffset);
  const stake = tradingState.currentStake;

  tradingState.waitingForResult = true;
  tradingState.tickCooldown = true;
  setTimeout(() => { tradingState.tickCooldown = false; }, 500); // avoid duplicate entries

  const buyParams = {
    buy: 1,
    price: Math.round(stake * 100) / 100,
    parameters: {
      contract_type: tradingState.direction,
      symbol: tradingState.market,
      duration: tradingState.duration,
      duration_unit: 't',
      barrier: Math.round(barrier * 100) / 100, // ensure 2 decimals
      amount: stake,
      basis: 'stake',
      currency: 'USD'
    }
  };

  addLog(`Placing ${tradingState.direction} ${tradingState.market} barrier=${barrier.toFixed(2)} stake=${stake} at tick ${currentPrice}`);
  derivWs.send(JSON.stringify(buyParams, null, 2));

  // Listen for buy response to get contract_id
  derivWs.once('message', (data) => {
    try {
      const resp = JSON.parse(data);
      if (resp.msg_type === 'buy' && resp.buy) {
        tradingState.pendingContractId = resp.buy.contract_id;
        addLog(`Contract ${resp.buy.contract_id} opened.`);
        // Subscribe to contract updates
        derivWs.send(JSON.stringify({ proposal_open_contract: 1, contract_id: resp.buy.contract_id }));
      } else if (resp.error) {
        addLog(`Buy error: ${resp.error.message}`);
        tradingState.waitingForResult = false;
        tradingState.pendingContractId = null;
      }
    } catch (e) { }
  });
}

// Start server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
