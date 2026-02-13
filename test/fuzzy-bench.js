#!/usr/bin/env node
/**
 * Fuzzy traffic generator + latency tracker using valkey-glide.
 *
 * Runs mixed commands (SET/GET/DEL/HSET/HGET/SADD/ZADD/LPUSH) on random keys
 * while measuring per-second latency statistics.
 *
 * Usage: node fuzzy-bench.js <host> <port> <seconds> [clients]
 */

const { GlideClient } = require("/home/ubuntu/work/valkey-glide/node");

const host = process.argv[2] || "127.0.0.1";
const port = parseInt(process.argv[3] || "6379");
const seconds = parseInt(process.argv[4] || "60");
const numClients = parseInt(process.argv[5] || "8");

let secOps = 0;
let secTotalUs = 0;
let secMaxUs = 0;
let secSamples = [];
const MAX_SAMPLES = 50000;

function hrToUs(hr) {
    return hr[0] * 1000000 + hr[1] / 1000;
}

function percentile(sorted, p) {
    const idx = Math.floor(sorted.length * p);
    return sorted[Math.min(idx, sorted.length - 1)];
}

function makeOp(client) {
    const r = Math.random();
    if (r < 0.2) return client.set(`fuzz:str:${Math.random().toString(36).slice(2, 10)}`, "v".repeat(256));
    if (r < 0.4) return client.get(`fuzz:str:${Math.random().toString(36).slice(2, 10)}`);
    if (r < 0.45) return client.del([`fuzz:str:${Math.random().toString(36).slice(2, 10)}`]);
    if (r < 0.55) return client.hset(`fuzz:hash:${Math.floor(Math.random() * 100)}`, { [`f${Math.floor(Math.random() * 1000)}`]: "v".repeat(100) });
    if (r < 0.65) return client.hget(`fuzz:hash:${Math.floor(Math.random() * 100)}`, `f${Math.floor(Math.random() * 1000)}`);
    if (r < 0.73) return client.sadd(`fuzz:set:${Math.floor(Math.random() * 50)}`, [`m${Math.floor(Math.random() * 10000)}`]);
    if (r < 0.80) return client.zadd(`fuzz:zset:${Math.floor(Math.random() * 50)}`, { [`e${Math.floor(Math.random() * 10000)}`]: Math.random() * 1000 });
    if (r < 0.85) return client.lpush(`fuzz:list:${Math.floor(Math.random() * 20)}`, [`item${Math.floor(Math.random() * 10000)}`]);
    if (r < 0.93) return client.set(`fuzz:big:${Math.floor(Math.random() * 500)}`, "X".repeat(4096));
    return client.get(`fuzz:big:${Math.floor(Math.random() * 500)}`);
}

const BATCH = 16;

async function runClient(client, stopRef) {
    while (!stopRef.stop) {
        const batch = [];
        for (let i = 0; i < BATCH; i++) {
            const start = process.hrtime();
            batch.push(
                makeOp(client)
                    .then(() => {
                        const us = hrToUs(process.hrtime(start));
                        secOps++;
                        secTotalUs += us;
                        if (us > secMaxUs) secMaxUs = us;
                        if (secSamples.length < MAX_SAMPLES) secSamples.push(us);
                    })
                    .catch(() => { secOps++; })
            );
        }
        await Promise.all(batch);
    }
}

async function main() {
    console.log(`# host=${host} port=${port} seconds=${seconds} clients=${numClients}`);
    console.log(`# sec  ops    avg_us  p50_us  p99_us  max_us`);

    const clients = [];
    for (let i = 0; i < numClients; i++) {
        const client = await GlideClient.createClient({
            addresses: [{ host, port }],
            clientName: `fuzzy-${i}`,
        });
        clients.push(client);
    }

    const stopRef = { stop: false };
    const promises = clients.map(c => runClient(c, stopRef));

    for (let sec = 0; sec < seconds; sec++) {
        secOps = 0;
        secTotalUs = 0;
        secMaxUs = 0;
        secSamples = [];

        await new Promise(r => setTimeout(r, 1000));

        const ops = secOps;
        const avg = ops > 0 ? Math.floor(secTotalUs / ops) : 0;
        const sorted = secSamples.slice().sort((a, b) => a - b);
        const p50 = sorted.length > 0 ? Math.floor(percentile(sorted, 0.5)) : 0;
        const p99 = sorted.length > 0 ? Math.floor(percentile(sorted, 0.99)) : 0;
        const max = Math.floor(secMaxUs);

        console.log(`${String(sec).padStart(4)}  ${String(ops).padStart(6)}  ${String(avg).padStart(6)}  ${String(p50).padStart(6)}  ${String(p99).padStart(6)}  ${String(max).padStart(6)}`);
    }

    stopRef.stop = true;
    await Promise.allSettled(promises);

    for (const c of clients) {
        c.close();
    }
}

main().catch(e => { console.error(e); process.exit(1); });
