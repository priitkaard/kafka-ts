import { spawnSync } from 'child_process';
import { readFileSync } from 'fs';
import { API, createKafkaClient, saslPlain } from 'kafka-ts';

const clients = ['kafka-ts', 'kafkajs', 'librdkafka'];
const runs = parseInt(process.env.RUNS ?? '3');

const scenarios: { name: string; env: Record<string, string>; metric: string }[] = [
    {
        name: 'Consume throughput (msg/s)',
        env: { PRELOAD: '1000000', MESSAGE_SIZE: '100' },
        metric: 'consumedPerSecond',
    },
    {
        name: 'Produce throughput (msg/s)',
        env: { PRODUCE_BATCH_SIZE: '1000', MESSAGE_SIZE: '100', DURATION_S: '20', CONSUMER: 'false' },
        metric: 'producedPerSecond',
    },
    {
        name: 'End-to-end latency avg (ms)',
        env: { PRODUCE_BATCH_SIZE: '10', PRODUCE_DELAY_MS: '10', MESSAGE_SIZE: '100', DURATION_S: '20' },
        metric: 'endToEndLatencyAvgMs',
    },
    {
        name: 'End-to-end latency p99 (ms)',
        env: { PRODUCE_BATCH_SIZE: '10', PRODUCE_DELAY_MS: '10', MESSAGE_SIZE: '100', DURATION_S: '20' },
        metric: 'endToEndLatencyP99Ms',
    },
];

const kafka = createKafkaClient({
    clientId: 'benchmark',
    bootstrapServers: [{ host: 'localhost', port: 39092 }],
    sasl: saslPlain({ username: 'admin', password: 'admin' }),
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});

const deleteTopic = async (topic: string) => {
    const cluster = kafka.createCluster();
    await cluster.connect();
    await cluster.setSeedBroker((await cluster.sendRequest(API.METADATA, { topics: [] })).controllerId);
    await cluster.sendRequest(API.DELETE_TOPICS, { topics: [{ name: topic, topicId: null }], timeoutMs: 10_000 });
    await cluster.disconnect();
};

const run = async (client: string, env: Record<string, string>) => {
    const topic = `benchmark-${client}-${Date.now()}`;
    const { stdout } = spawnSync('node', [`${__dirname}/${client}.js`], {
        env: { ...process.env, ...env, TOPIC: topic, GROUP_ID: topic },
        encoding: 'utf-8',
        maxBuffer: 64 * 1024 * 1024,
    });
    await deleteTopic(topic);
    const line = stdout.split('\n').find((line) => line.startsWith('RESULT '));
    if (!line) throw new Error(`${client} did not report a result`);
    return JSON.parse(line.slice('RESULT '.length)) as Record<string, number>;
};

const median = (values: number[]) => values.toSorted((a, b) => a - b)[Math.floor(values.length / 2)];

const main = async () => {
    const results: Record<string, Record<string, number>> = {};
    const cache = new Map<string, Record<string, number>[]>();
    for (const scenario of scenarios) {
        for (const client of clients) {
            const key = `${client} ${JSON.stringify(scenario.env)}`;
            if (!cache.has(key)) {
                const runResults = [];
                for (let i = 0; i < runs; i++) runResults.push(await run(client, scenario.env));
                cache.set(key, runResults);
            }
            results[scenario.name] ??= {};
            results[scenario.name][client] = median(cache.get(key)!.map((result) => result[scenario.metric]));
            console.error(`${scenario.name} | ${client}: ${results[scenario.name][client]}`);
        }
    }

    console.log(`| | ${clients.join(' | ')} |`);
    console.log(`| --- | ${clients.map(() => '---:').join(' | ')} |`);
    for (const [name, values] of Object.entries(results)) {
        console.log(`| ${name} | ${clients.map((client) => values[client].toLocaleString('en-US')).join(' | ')} |`);
    }
};

main();
