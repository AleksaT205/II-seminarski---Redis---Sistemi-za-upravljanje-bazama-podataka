'use strict';

const { execSync, exec } = require('child_process');
const Redis = require('ioredis');

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const sentinelHosts = [
  { host: 'localhost', port: 26379 },
  { host: 'localhost', port: 26380 },
  { host: 'localhost', port: 26381 },
];

async function getMasterAddr(sentinel) {
  const addr = await sentinel.call('SENTINEL', 'get-master-addr-by-name', 'mymaster');
  return { host: addr[0], port: parseInt(addr[1]) };
}

async function runFailoverTest() {
  console.log('\n' + '═'.repeat(60));
  console.log('  REDIS SENTINEL FAILOVER TEST');
  console.log('  Seminarski rad - Aleksandar S. Stanimirović');
  console.log('═'.repeat(60) + '\n');

  const sentinel = new Redis({ host: 'localhost', port: 26379, connectTimeout: 5000 });
  const client = new Redis({
    sentinels: sentinelHosts,
    name: 'mymaster',
    retryStrategy: (times) => Math.min(times * 100, 2000),
    sentinelRetryStrategy: (times) => Math.min(times * 200, 3000),
  });

  let failoverDetected = false;
  client.on('sentinelMasterSwitch', (master) => {
    failoverDetected = true;
    console.log(`\n [KLIJENT] Master switch event primljen!`);
  });

  try {
    console.log('FAZA 1: Početno stanje\n');
    const initialMaster = await getMasterAddr(sentinel);
    console.log(`  Master: ${initialMaster.host}:${initialMaster.port}`);

    const slavesRaw = await sentinel.call('SENTINEL', 'slaves', 'mymaster');
    const slaves = [];
    for (let i = 0; i < slavesRaw.length; i++) {
      const s = {};
      slavesRaw[i].forEach((v, j) => { if (j % 2 === 0) s[v] = slavesRaw[i][j + 1]; });
      slaves.push(s);
    }
    console.log(`  Replike: ${slaves.length}`);
    slaves.forEach((s, i) => console.log(`    Replika ${i+1}: ${s.ip}:${s.port} [${s.flags}]`));

    await client.set('failover-test-key', 'initial-value', 'EX', 600);
    const dbsizeBefore = await client.dbsize();
    console.log(`\n  Test ključ upisan: "failover-test-key" = "initial-value"`);
    console.log(`  Ukupno ključeva pre failovera: ${dbsizeBefore}`);

    console.log('\nFAZA 2: Upis podataka tokom failovera\n');

    let writeCount = 0;
    let errorCount = 0;
    let keepWriting = true;

    const writeLoop = async () => {
      while (keepWriting) {
        try {
          await client.set(`live:key:${writeCount}`, Date.now().toString(), 'EX', 300);
          writeCount++;
          await sleep(100);
        } catch (e) {
          errorCount++;
          await sleep(200);
        }
      }
    };
    writeLoop(); 

    console.log('FAZA 3: Simulacija otkaza mastera\n');
    console.log(`  Pauziranje kontejnera "redis-master"...`);

    const failoverStart = Date.now();
    try {
      execSync('docker pause redis-master', { stdio: 'ignore' });
      console.log(' redis-master je pauziran (simulacija otkaza)');
    } catch (e) {
      console.log('  Nije moguće pauzirati Docker kontejner. Da li je Docker pokrenut?');
      console.log(' Nastavljam sa monitoring-om...');
    }

    console.log('\n FAZA 4: Praćenje failover procesa\n');

    let newMaster = null;
    let dotCount = 0;

    process.stdout.write('  Čekanje...');
    for (let i = 0; i < 120; i++) {
      await sleep(500);
      try {
        const current = await getMasterAddr(sentinel);
        if (current.port !== initialMaster.port) {
          newMaster = current;
          break;
        }
      } catch (e) {}

      process.stdout.write('.');
      dotCount++;
      if (dotCount % 20 === 0) process.stdout.write('\n  ');
    }
    console.log();

    const failoverTime = Date.now() - failoverStart;
    keepWriting = false;

    if (newMaster) {
      console.log(`\n FAILOVER USPEŠAN!`);
      console.log(`   Vreme failovera: ${failoverTime}ms (${(failoverTime/1000).toFixed(1)}s)`);
      console.log(` Novi master: ${newMaster.host}:${newMaster.port}`);
    } else {
      console.log(`\n Failover nije detektovan u roku od 60 sekundi`);
      console.log(`  Proveri da li je Docker pokrenut i da li sentinel config odgovara.`);
    }

    console.log(`\n  Upisi tokom failovera: ${writeCount} uspešnih, ${errorCount} grešaka`);

    console.log('\n FAZA 5: Verifikacija podataka\n');

    await sleep(2000);

    try {
      const value = await client.get('failover-test-key');
      console.log(`  Test ključ dostupan: ${value === 'initial-value' ? 'DA' : 'NE'} (vrednost: ${value})`);
    } catch (e) {
      console.log(` Greška pri čitanju test ključa: ${e.message}`);
    }

    try {
      const dbsizeAfter = await client.dbsize();
      console.log(`  Ključevi posle failovera: ${dbsizeAfter} (pre: ${dbsizeBefore})`);
    } catch (e) {
      console.log(` Greška pri čitanju dbsize: ${e.message}`);
    }

    console.log('\n FAZA 6: Oporavak starog master čvora\n');
    try {
      execSync('docker unpause redis-master', { stdio: 'ignore' });
      console.log('  redis-master vraćen u rad');
      await sleep(3000);
      console.log(' Stari master sada postaje replika novog mastera...');
    } catch (e) {
      console.log(' Kontejner nije pronađen (možda nije pauziran)');
    }

    console.log('\n' + '═'.repeat(60));
    console.log('  REZIME TESTA');
    console.log('═'.repeat(60));
    console.log(`  Inicijalni master:    ${initialMaster.host}:${initialMaster.port}`);
    console.log(`  Novi master:          ${newMaster ? `${newMaster.host}:${newMaster.port}` : 'N/A'}`);
    console.log(`  Vreme failovera:      ${(failoverTime/1000).toFixed(1)}s`);
    console.log(`  Uspešni upisi:        ${writeCount}`);
    console.log(`  Greške tokom FO:      ${errorCount}`);
    console.log(`  Failover event:       ${failoverDetected ? ' primljen' : '  nije primljen'}`);
    console.log('═'.repeat(60) + '\n');

  } catch (err) {
    console.error('\n Greška tokom testa:', err.message);
  } finally {
    sentinel.disconnect();
    client.disconnect();
  }
}

runFailoverTest();
