'use strict';

const Redis = require('ioredis');

const sentinelHosts = (process.env.SENTINEL_HOSTS || 'localhost:26379,localhost:26380,localhost:26381')
  .split(',')
  .map(h => {
    const [host, port] = h.trim().split(':');
    return { host, port: parseInt(port, 10) };
  });

const masterName = process.env.MASTER_NAME || 'mymaster';

function createMasterClient() {
  return new Redis({
    sentinels: sentinelHosts,
    name: masterName,
    role: 'master',
    retryStrategy: (times) => Math.min(times * 100, 3000),
    sentinelRetryStrategy: (times) => Math.min(times * 200, 5000),
    enableReadyCheck: true,
    maxRetriesPerRequest: 3,
    lazyConnect: false,
  });
}

function createReplicaClient() {
  return new Redis({
    sentinels: sentinelHosts,
    name: masterName,
    role: 'slave',
    retryStrategy: (times) => Math.min(times * 100, 3000),
    sentinelRetryStrategy: (times) => Math.min(times * 200, 5000),
    enableReadyCheck: true,
    maxRetriesPerRequest: 3,
    lazyConnect: false,
  });
}

const masterClient = createMasterClient();
const replicaClient = createReplicaClient();

const EventEmitter = require('events');
const events = new EventEmitter();

masterClient.on('error', (err) => {
  console.error('[MASTER ERROR]', err.message);
});

masterClient.on('connect', () => {
  console.log('[MASTER] Konektovan');
});

masterClient.on('ready', () => {
  console.log('[MASTER] Spreman');
});

masterClient.on('sentinelMasterSwitch', (master) => {
  console.log('[SENTINEL] Master switch:', master);
  events.emit('failover', { newMaster: master, timestamp: Date.now() });
});

replicaClient.on('error', (err) => {
  console.error('[REPLICA ERROR]', err.message);
});

// MERENJE REPLIKACIONOG KAŠNJENJA 
async function measureReplicationLag() {
  const timestamp = Date.now();
  const key = `lag:probe:${timestamp}:${Math.random().toString(36).slice(2)}`;

  try {
    await masterClient.set(key, String(timestamp), 'PX', 10000);
    const writeTime = Date.now();

    for (let i = 0; i < 100; i++) {
      await sleep(5);
      const value = await replicaClient.get(key);
      if (value !== null) {
        const lagMs = Date.now() - writeTime;
        await masterClient.del(key);
        return { lag_ms: lagMs, status: 'ok' };
      }
    }

    await masterClient.del(key).catch(() => {});
    return { lag_ms: -1, status: 'timeout' };
  } catch (err) {
    return { lag_ms: -1, status: 'error', error: err.message };
  }
}

// INFORMACIJE O STATUSU
async function getReplicationInfo() {
  try {
    const info = await masterClient.info('replication');
    return parseRedisInfo(info);
  } catch (err) {
    return { error: err.message };
  }
}

// BENCHMARK 
async function runBenchmark(ops = 1000, pipelineSize = 100) {
  const results = { ops, pipeline_size: pipelineSize };

  const writeStart = Date.now();
  for (let i = 0; i < ops; i += pipelineSize) {
    const pipeline = masterClient.pipeline();
    const batch = Math.min(pipelineSize, ops - i);
    for (let j = 0; j < batch; j++) {
      pipeline.set(`bench:${i + j}`, `value:${i + j}:${Date.now()}`, 'EX', 120);
    }
    await pipeline.exec();
  }
  results.write_duration_ms = Date.now() - writeStart;
  results.write_ops_per_sec = Math.round(ops / (results.write_duration_ms / 1000));

  const readStart = Date.now();
  for (let i = 0; i < ops; i += pipelineSize) {
    const pipeline = masterClient.pipeline();
    const batch = Math.min(pipelineSize, ops - i);
    for (let j = 0; j < batch; j++) {
      pipeline.get(`bench:${i + j}`);
    }
    await pipeline.exec();
  }
  results.read_duration_ms = Date.now() - readStart;
  results.read_ops_per_sec = Math.round(ops / (results.read_duration_ms / 1000));

  const pipeline = masterClient.pipeline();
  for (let i = 0; i < ops; i++) {
    pipeline.del(`bench:${i}`);
  }
  await pipeline.exec();

  return results;
}

// PARSIRANJE INFO 
function parseRedisInfo(info) {
  const result = {};
  info.split('\r\n').forEach(line => {
    if (line && !line.startsWith('#')) {
      const [key, value] = line.split(':');
      if (key && value !== undefined) result[key.trim()] = value.trim();
    }
  });
  return result;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports = {
  masterClient,
  replicaClient,
  events,
  sentinelHosts,
  masterName,
  measureReplicationLag,
  getReplicationInfo,
  runBenchmark,
  parseRedisInfo,
};
