#!/usr/bin/env node
/**
 * Fuzzy traffic generator + latency tracker using valkey-glide.
 *
 * Runs mixed commands (SET/GET/DEL/HSET/HGET/SADD/ZADD/LPUSH) on random keys
 * while measuring per-second latency statistics.
 *
 * Usage: node fuzzy-bench.mjs <host> <port> <seconds> [clients]
 */

import { GlideClient } from "/home/ubuntu/work/valkey-glide/node/build-ts/GlideClient.js";

const host = process.argv[2] || "127.0.0.1";
const port = parseInt(process.argv[3] || "6379");
const seconds = parseInt(process.argv[4] || "60");
const numClients = parseInt(process.argv[5] || "8");

// Per-second stats
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

async function runClient(client, stopRef) {
    const ops = [
        async () => { await client.set(`fuzz:str:${Math.random().toString(36).slice(2, 10)}`, "v".repeat(256)); },
        async () => { await client.get(`fuzz:str:${Math.random().toString(36).slice(2, 10)}`); },
        async () => { await client.del([`fuzz:str:${Math.random().toString(36).slice(2, 10)}`]); },
        async () => { await client.hset(`fuzz:hash:${Math.floor(Math.random() * 100)}`, { [`f${Math.floor(Math.random() * 1000)}`]: "v".repeat(100) }); },
        async () => { await client.hget(`fuzz:hash:${Math.floor(Math.random() * 100)}`, `f${Math.floor(Math.random() * 1000)}`); },
        async () => { await client.sadd(`fuzz:set:${Math.floor(Math.random() * 50)}`, [`m${Math.floor(Math.random() * 10000)}`]); },
        async () => { await client.zadd(`fuzz:zset:${Math.floor(Math.random() * 50)}`, { [`e${Math.floor(Math.random() * 10000)}`]: Math.random() * 1000 }); },
        async () => { await client.lpush(`fuzz:list:${Math.floor(Math.random() * 20)}`, [`item${Math.floor(Math.random() * 10000)}`]); },
        async () => { await client.set(`fuzz:big:${Math.floor(Math.random() * 500)}`, "X".repeat(4096)); },
        async () => { await client.get(`fuzz:big:${Math.floor(Math.random() * 500)}`); },
    ];

    while (!stopRef.stop) {
        const op = ops[Math.floor(Math.random() * ops.length)];
        const start = process.hrtime();
        try {
            await op();
        } catch (e) {
            // Connection errors during CRIU freeze — just continue
            continue;
        }
        const elapsed = process.hrtime(start);
        const us = hrToUs(elapsed);

        secOps++;
        secTotalUs += us;
        if (us > secMaxUs) secMaxUs = us;
        if (secSamples.length < MAX_SAMPLES) secSamples.push(us);
    }
}

async function main() {
    console.log(`# host=${host} port=${port} seconds=${seconds} clients=${numClients}`);
    console.log(`# sec  ops    avg_us  p50_us  p99_us  max_us`);

    // Connect clients
    const clients = [];
    for (let i = 0; i < numClients; i++) {
        const client = await GlideClient.createClient({
            addresses: [{ host, port }],
            clientName: `fuzzy-${i}`,
        });
        clients.push(client);
    }

    const stopRef = { stop: false };
    const startTime = Date.now();

    // Start all clients
    const promises = clients.map(c => runClient(c, stopRef));

    // Report per-second stats
    for (let sec = 0; sec < seconds; sec++) {
        // Reset counters
        secOps = 0;
        secTotalUs = 0;
        secMaxUs = 0;
        secSamples = [];

        // Wait 1 second
        await new Promise(r => setTimeout(r, 1000));

        // Compute stats
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
