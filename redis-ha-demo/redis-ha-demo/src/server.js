'use strict';

const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');
const Redis = require('ioredis');

const {
  masterClient,
  replicaClient,
  events,
  sentinelHosts,
  masterName,
  measureReplicationLag,
  getReplicationInfo,
  runBenchmark,
  parseRedisInfo,
} = require('./redis-client');

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: { origin: '*' }
});

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

async function getSentinelClient() {
  const s = sentinelHosts[0];
  return new Redis({ host: s.host, port: s.port, connectTimeout: 3000 });
}

function parseSentinelList(arr) {
  if (!Array.isArray(arr)) return {};
  const obj = {};
  for (let i = 0; i < arr.length; i += 2) {
    obj[arr[i]] = arr[i + 1];
  }
  return obj;
}

app.get('/api/cluster-status', async (req, res) => {
  let sentinel = null;
  try {
    sentinel = await getSentinelClient();

    const [masterInfo, slavesRaw, sentinelsRaw] = await Promise.all([
      sentinel.call('SENTINEL', 'master', masterName).catch(() => null),
      sentinel.call('SENTINEL', 'slaves', masterName).catch(() => []),
      sentinel.call('SENTINEL', 'sentinels', masterName).catch(() => []),
    ]);

    const master = masterInfo ? parseSentinelList(masterInfo) : null;
    const slaves = Array.isArray(slavesRaw) ? slavesRaw.map(s => parseSentinelList(s)) : [];
    const sentinels = Array.isArray(sentinelsRaw) ? sentinelsRaw.map(s => parseSentinelList(s)) : [];

    let replInfo = {};
    try {
      const info = await masterClient.info('replication');
      replInfo = parseRedisInfo(info);
    } catch (e) {}

    res.json({
      ok: true,
      master,
      slaves,
      sentinels,
      replication: replInfo,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  } finally {
    if (sentinel) sentinel.disconnect();
  }
});

app.get('/api/replication-lag', async (req, res) => {
  try {
    const result = await measureReplicationLag();
    res.json({ ok: true, ...result, timestamp: new Date().toISOString() });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/info/:section?', async (req, res) => {
  try {
    const section = req.params.section || 'all';
    const info = await masterClient.info(section);
    const parsed = parseRedisInfo(info);
    res.json({ ok: true, section, data: parsed });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/benchmark', async (req, res) => {
  try {
    const { ops = 500, pipelineSize = 100 } = req.body;
    const capped = Math.min(ops, 5000); // max 5000 za sigurnost
    const result = await runBenchmark(capped, pipelineSize);
    res.json({ ok: true, ...result });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.post('/api/write-test', async (req, res) => {
  try {
    const { count = 10 } = req.body;
    const pipeline = masterClient.pipeline();
    for (let i = 0; i < count; i++) {
      pipeline.set(
        `test:key:${Date.now()}:${i}`,
        JSON.stringify({ value: Math.random(), ts: Date.now() }),
        'EX',
        300
      );
    }
    await pipeline.exec();
    const dbsize = await masterClient.dbsize();
    res.json({ ok: true, written: count, total_keys: dbsize });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/key-count', async (req, res) => {
  try {
    const [masterCount, replicaCount] = await Promise.all([
      masterClient.dbsize().catch(() => -1),
      replicaClient.dbsize().catch(() => -1),
    ]);
    res.json({ ok: true, master: masterCount, replica: replicaCount });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

app.get('/api/current-master', async (req, res) => {
  let sentinel = null;
  try {
    sentinel = await getSentinelClient();
    const addr = await sentinel.call('SENTINEL', 'get-master-addr-by-name', masterName);
    res.json({ ok: true, host: addr[0], port: addr[1] });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  } finally {
    if (sentinel) sentinel.disconnect();
  }
});

const metricsHistory = [];
const MAX_HISTORY = 60;

async function collectMetrics() {
  try {
    const [lagResult, keyCountMaster] = await Promise.all([
      measureReplicationLag(),
      masterClient.dbsize().catch(() => 0),
    ]);

    let info = {};
    try {
      const raw = await masterClient.info('all');
      info = parseRedisInfo(raw);
    } catch (e) {}

    const metric = {
      timestamp: Date.now(),
      lag_ms: lagResult.lag_ms,
      lag_status: lagResult.status,
      total_keys: keyCountMaster,
      used_memory_human: info.used_memory_human || 'N/A',
      connected_clients: parseInt(info.connected_clients) || 0,
      ops_per_sec: parseInt(info.instantaneous_ops_per_sec) || 0,
      hit_rate: info.keyspace_hits && info.keyspace_misses
        ? Math.round(
            parseInt(info.keyspace_hits) /
            (parseInt(info.keyspace_hits) + parseInt(info.keyspace_misses)) * 100
          )
        : 0,
      role: info.role || 'unknown',
      connected_slaves: parseInt(info.connected_slaves) || 0,
    };

    metricsHistory.push(metric);
    if (metricsHistory.length > MAX_HISTORY) metricsHistory.shift();

    io.emit('metrics', metric);
  } catch (err) {
  }
}

setInterval(collectMetrics, 2000);

events.on('failover', (data) => {
  io.emit('failover', data);
  console.log('[FAILOVER EVENT]', data);
});

app.get('/api/metrics-history', (req, res) => {
  res.json({ ok: true, history: metricsHistory });
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════╗
║   Redis HA Monitor - Seminarski demo             ║
║   Dashboard: http://localhost:${PORT}               ║
╚══════════════════════════════════════════════════╝
  `);
  setTimeout(collectMetrics, 2000);
});
