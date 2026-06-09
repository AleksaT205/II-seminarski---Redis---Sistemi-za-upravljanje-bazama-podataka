// Merenje replikacionog kašnjenja pod različitim opterećenjima
'use strict';

const Redis = require('ioredis');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const master = new Redis({ host: 'localhost', port: 6379 });
const replica = new Redis({ host: 'localhost', port: 6380 });

async function measureLag(label, preWriteFn) {
  if (preWriteFn) await preWriteFn();
  
  const samples = [];
  for (let i = 0; i < 20; i++) {
    const key = `lag:test:${Date.now()}:${i}`;
    const ts = Date.now();
    await master.set(key, String(ts), 'PX', 10000);
    const writeTs = Date.now();

    let lag = -1;
    for (let j = 0; j < 200; j++) {
      await sleep(2);
      const val = await replica.get(key);
      if (val !== null) { lag = Date.now() - writeTs; break; }
    }
    if (lag >= 0) samples.push(lag);
    await master.del(key).catch(() => {});
    await sleep(50);
  }

  if (samples.length === 0) {
    console.log(`  ${label}: N/A (replika nije odgovorila)`);
    return;
  }

  samples.sort((a, b) => a - b);
  const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
  const p50 = samples[Math.floor(samples.length * 0.5)];
  const p95 = samples[Math.floor(samples.length * 0.95)] || samples[samples.length-1];
  const min = samples[0];
  const max = samples[samples.length - 1];

  console.log(`  ${label}`);
  console.log(`    Min: ${min}ms  Avg: ${avg.toFixed(1)}ms  P50: ${p50}ms  P95: ${p95}ms  Max: ${max}ms`);
  return { label, avg, p50, p95, min, max };
}

async function writeLoad(opsPerSec, durationMs) {
  const interval = 1000 / opsPerSec;
  const end = Date.now() + durationMs;
  let count = 0;
  while (Date.now() < end) {
    await master.set(`load:${count++}`, Date.now(), 'EX', 30).catch(() => {});
    await sleep(interval);
  }
}

async function run() {
  console.log('\n' + '═'.repeat(60));
  console.log('  MERENJE REPLIKACIONOG KAŠNJENJA');
  console.log('  Redis 7.2 · Master :6379 → Replika :6380');
  console.log('═'.repeat(60) + '\n');

  // Provera konekcije
  try {
    await master.ping();
    await replica.ping();
  } catch (e) {
    console.error('❌ Nije moguće konektovati na Redis. Da li je infra pokrenuta?');
    console.error('   Pokreni: npm run infra:up');
    process.exit(1);
  }

  console.log('📊 Scenario 1: Bez opterećenja (idle)');
  await measureLag('Idle kašnjenje');

  console.log('\n📊 Scenario 2: Nisko opterećenje (100 ops/s)');
  const loadPromise1 = writeLoad(100, 3000);
  await measureLag('100 ops/s opterećenje');
  await loadPromise1;

  console.log('\n📊 Scenario 3: Visoko opterećenje (500 ops/s)');
  const loadPromise2 = writeLoad(500, 5000);
  await measureLag('500 ops/s opterećenje');
  await loadPromise2;

  console.log('\n📊 Scenario 4: Pipeline upis (1000 pipeline ops)');
  const bigPipeline = async () => {
    const pipeline = master.pipeline();
    for (let i = 0; i < 1000; i++) pipeline.set(`pipe:${i}`, Date.now(), 'EX', 60);
    await pipeline.exec();
  };
  await measureLag('Posle 1000-pipeline upisa', bigPipeline);

  // Čišćenje
  const pipeline = master.pipeline();
  for (let i = 0; i < 1000; i++) pipeline.del(`pipe:${i}`);
  await pipeline.exec();

  console.log('\n' + '═'.repeat(60));
  console.log('  Test završen.');
  console.log('═'.repeat(60) + '\n');

  master.disconnect();
  replica.disconnect();
}

run().catch(console.error);
