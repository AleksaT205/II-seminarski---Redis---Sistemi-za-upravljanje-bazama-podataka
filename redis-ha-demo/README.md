# Redis HA Monitor

> **Seminarski rad:** Replikacija i visoka dostupnost u Redis Sentinel/Cluster arhitekturi
> **Profesor:** Aleksandar S. Stanimirović
> **Student:** Aleksa Tomić, broj indeksa 1812 
> **Predmet:** Sistemi za upravljanje bazama podataka
> **Elektronski fakultet Niš · 2026.**

---

## O projektu

Redis HA Monitor je demo aplikacija koja vizuelizuje stanje Redis Sentinel klastera u realnom vremenu. Aplikacija demonstrira ključne koncepte replikacije i visoke dostupnosti u Redis sistemu — automatski failover, merenje replikacionog kašnjenja i monitoring topologije klastera.

### Šta demonstrira

- Redis master-replica replikaciju (asinhroni model)
- Redis Sentinel monitoring i automatski failover
- Merenje replikacionog kašnjenja u realnom vremenu
- Benchmark write/read operacija sa pipeline-om
- Live dashboard sa WebSocket push metrikama

---

## Tehnologije

| Komponenta | Tehnologija |
|---|---|
| Baza podataka | Redis 7.2 (ili 3.0 na Windows) |
| Backend | Node.js 20 + Express 4 |
| Realtime komunikacija | Socket.IO 4 |
| Redis klijent | ioredis 5 |
| Infrastruktura | Docker Compose (Linux/macOS) |
| Frontend | Vanilla HTML/CSS/JS |

---

## Pokretanje na Windows (bez Dockera)

### Preduslovi

- **Node.js 18+** — https://nodejs.org
- **Redis za Windows** — instalacija komandom:

```bash
winget install Redis.Redis
```

### Korak 1 — Instaliraj Node zavisnosti

```bash
cd redis-ha-demo
npm install
```

### Korak 2 — Pokreni Redis Master

Otvori novi CMD prozor:

```bash
"C:\Program Files\Redis\redis-server.exe" --port 6379 --bind 127.0.0.1
```

### Korak 3 — Pokreni Redis Repliku

Otvori novi CMD prozor:

```bash
"C:\Program Files\Redis\redis-server.exe" --port 6380 --bind 127.0.0.1 --slaveof 127.0.0.1 6379
```

Opciono — druga replika (novi CMD):

```bash
"C:\Program Files\Redis\redis-server.exe" --port 6381 --bind 127.0.0.1 --slaveof 127.0.0.1 6379
```

### Korak 4 — Napravi Sentinel konfiguraciju

Otvori Notepad i sačuvaj fajl kao `C:\Users\<tvoje-ime>\Desktop\sentinel.conf` sa sadržajem:

```
port 26379
sentinel monitor mymaster 127.0.0.1 6379 1
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 30000
sentinel parallel-syncs mymaster 1
```

### Korak 5 — Pokreni Sentinel

Otvori novi CMD prozor:

```bash
"C:\Program Files\Redis\redis-server.exe" "%USERPROFILE%\Desktop\sentinel.conf" --sentinel
```

### Korak 6 — Pokreni aplikaciju

```bash
npm start
```

### Korak 7 — Otvori dashboard

```
http://localhost:3000
```

---

## Pokretanje na Linux / macOS (Docker)

### Preduslovi

- Docker Desktop
- Node.js 18+

### Pokretanje

```bash
# Instaliraj zavisnosti
npm install

# Pokreni Redis infrastrukturu (6 kontejnera)
npm run infra:up

# Sačekaj ~15 sekundi, zatim pokreni app
npm start
```

Otvori browser: **http://localhost:3000**

### Zaustavljanje

```bash
# Ctrl+C za Node app, zatim:
npm run infra:down
```

---

## Struktura projekta

```
redis-ha-demo/
├── docker-compose.yml        # Redis infrastruktura (6 kontejnera)
├── Dockerfile                # Node.js app kontejner
├── package.json
├── redis/
│   ├── master.conf           # Redis master konfiguracija
│   ├── replica.conf          # Redis replica konfiguracija
│   └── sentinel.conf         # Sentinel konfiguracija
├── src/
│   ├── server.js             # Express + Socket.IO backend
│   ├── redis-client.js       # Sentinel-aware Redis klijent
│   └── public/
│       └── index.html        # Dashboard UI
└── tests/
    ├── failover-test.js      # Automatski failover test
    └── replication-lag.js    # Merenje replikacionog kašnjenja
```

---

## API endpointi

| Metoda | Endpoint | Opis |
|--------|----------|------|
| GET | `/api/cluster-status` | Kompletan status klastera |
| GET | `/api/replication-lag` | Trenutno replikaciono kašnjenje |
| GET | `/api/info/:section` | Redis INFO za datu sekciju |
| GET | `/api/key-count` | Broj ključeva na masteru i replici |
| GET | `/api/current-master` | Trenutna adresa mastera |
| POST | `/api/benchmark` | Pokretanje benchmark testa |
| POST | `/api/write-test` | Upisivanje test podataka |
| GET | `/api/metrics-history` | Istorija metrika (poslednjih 60) |

---

## Test skripte

### Merenje replikacionog kašnjenja

```bash
npm run test:lag
```

Meri kašnjenje u 4 scenarija: idle, 100 ops/s, 500 ops/s i posle pipeline upisa. Prikazuje min/avg/p50/p95/max vrednosti.

### Failover test

```bash
npm run test:failover
```

> ⚠️ Zahteva Docker. Na Windows sa lokalnim Redis-om failover se simulira komandom:
> `"C:\Program Files\Redis\redis-cli.exe" -p 6379 DEBUG sleep 30`

Skripta:
1. Beleži inicijalnog mastera
2. Pauzira `redis-master` kontejner
3. Prati Sentinel dok ne detektuje novi master
4. Meri vreme failovera
5. Verifikuje da su podaci dostupni
6. Vraća stari master kao repliku

---

## Arhitektura sistema

```
┌─────────────────────────────────────────────┐
│  Klijenti (Node.js app)                     │
│                                             │
│  ioredis Sentinel klijent                   │
│    ├── Master klijent  (write)              │
│    └── Replica klijent (read)               │
└──────────┬──────────────────────────────────┘
           │ Sentinel protokol
           ▼
┌─────────────────────────────────────────────┐
│  Redis Sentinel (:26379)                    │
│  Monitoring · Failover · Config provider    │
└──────────┬──────────────────────────────────┘
           │
    ┌──────┴───────┐
    ▼              ▼
┌────────┐    ┌──────────┐
│ Master │───►│ Replika  │
│ :6379  │    │ :6380    │
└────────┘    └──────────┘
  write          read
```

---

## Eksperimentalni rezultati

Mereno lokalno · Windows 10 · Redis 3.0 · Node.js 20

| Merenje | Vrednost |
|---------|----------|
| Replikaciono kašnjenje (idle) | 16 ms |
| Ops/sec (baseline) | 2–10 |
| Write ops/sec (pipeline 100) | 166,667 |
| Read ops/sec | 250,000 |
| Failover vreme (Sentinel) | 8–15 sekundi |

---

## Licenca

MIT — slobodno koristi za obrazovne svrhe.

