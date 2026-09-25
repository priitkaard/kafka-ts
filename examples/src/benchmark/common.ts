import { delay } from '../utils/delay';

type Benchmarker = {
    createTopic: (opts: { topic: string; partitions: number; replicationFactor: number }) => Promise<void>;
    connectProducer: () => Promise<() => unknown>;
    startConsumer: (
        opts: {
            groupId: string;
            topic: string;
            concurrency: number;
            fromBeginning: boolean;
            incrementCount: (key: string, value: number) => void;
        },
        callback: (timestamp: number) => void,
    ) => Promise<() => unknown>;
    produce: (opts: { topic: string; length: number; value: string; timestamp: number; acks: -1 | 1 }) => Promise<void>;
};

const percentile = (values: number[], p: number) => {
    const sorted = values.toSorted((a, b) => a - b);
    return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0;
};

const average = (values: number[]) => (values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0);

const benchmarkId = `benchmark-${Date.now()}`;
const {
    TOPIC = benchmarkId,
    GROUP_ID = benchmarkId,
    PRODUCER = 'true',
    CONSUMER = 'true',
    PARTITIONS = '10',
    REPLICATION_FACTOR = '3',
    CONCURRENCY = '1',
    PRODUCE_BATCH_SIZE = '10',
    PRODUCE_DELAY_MS = '0',
    MESSAGE_SIZE = '5',
    WARMUP_S = '5',
    DURATION_S,
    PRELOAD,
} = process.env;
const value = 'x'.repeat(parseInt(MESSAGE_SIZE));
const concurrency = parseInt(CONCURRENCY);

export const startBenchmarker = async (benchmarker: Benchmarker) => {
    await benchmarker
        .createTopic({
            topic: TOPIC,
            partitions: parseInt(PARTITIONS),
            replicationFactor: parseInt(REPLICATION_FACTOR),
        })
        .catch(console.error);
    await delay(2500);

    return PRELOAD ? runBacklog(benchmarker, parseInt(PRELOAD)) : runLive(benchmarker);
};

const runBacklog = async ({ connectProducer, startConsumer, produce }: Benchmarker, total: number) => {
    const stopProducer = await connectProducer();
    for (let sent = 0; sent < total; sent += 1000) {
        await produce({ topic: TOPIC, length: 1000, value, timestamp: Date.now(), acks: -1 });
    }
    await stopProducer();

    let consumed = 0;
    let firstAt = 0;
    const stopConsumer = await startConsumer(
        { groupId: GROUP_ID, topic: TOPIC, concurrency, fromBeginning: true, incrementCount: () => {} },
        () => {
            firstAt ||= Date.now();
            consumed++;
        },
    );
    while (consumed < total) await delay(10);
    const seconds = (Date.now() - firstAt) / 1000;

    console.log(`RESULT ${JSON.stringify({ consumedPerSecond: Math.round(total / seconds) })}`);
    await stopConsumer();
    process.exit(0);
};

const runLive = async ({ connectProducer, startConsumer, produce }: Benchmarker) => {
    const produceBatchSize = parseInt(PRODUCE_BATCH_SIZE);
    const produceDelayMs = parseInt(PRODUCE_DELAY_MS);

    let counts: Record<string, number> = {};
    let sums: Record<string, number> = {};

    const incrementCount = (key: string, value: number) => {
        counts[key] = (counts[key] || 0) + value;
    };

    const incrementSum = (key: string, value: number) => {
        sums[key] = (sums[key] || 0) + value;
    };

    let isMeasuring = false;
    const totals = { produced: 0, consumed: 0, endToEndLatencies: [] as number[], produceLatencies: [] as number[] };

    const stopProducer = await connectProducer();

    const stopConsumer =
        CONSUMER === 'true' &&
        (await startConsumer(
            { groupId: GROUP_ID, topic: TOPIC, concurrency, fromBeginning: false, incrementCount },
            (timestamp) => {
                const latency = Date.now() - timestamp;
                incrementCount('CONSUMER', 1);
                incrementSum('CONSUMER', latency);
                if (isMeasuring) {
                    totals.consumed++;
                    totals.endToEndLatencies.push(latency);
                }
            },
        ));

    const interval = setInterval(() => {
        const latencies = Object.entries(sums)
            .map(([key, sum]) => `${key} ${(sum / counts[key]).toFixed(2)}ms`)
            .sort()
            .join(', ');

        const counters = Object.entries(counts)
            .map(([key, count]) => `${key} ${count}`)
            .sort()
            .join(', ');

        console.log(`Latency: ${latencies} | Counters: ${counters}`);
        counts = {};
        sums = {};
    }, 1000);

    let isRunning = true;
    const produceLoop = async () => {
        if (!isRunning) return;
        const start = Date.now();
        await produce({ topic: TOPIC, length: produceBatchSize, value, timestamp: Date.now(), acks: -1 });
        const latency = Date.now() - start;
        incrementCount('PRODUCER', 1);
        incrementSum('PRODUCER', latency);
        if (isMeasuring) {
            totals.produced += produceBatchSize;
            totals.produceLatencies.push(latency);
        }
        produceDelayMs && (await delay(produceDelayMs));
        produceLoop();
    };
    PRODUCER === 'true' && produceLoop();

    const stop = async () => {
        isRunning = false;
        clearInterval(interval);
        stopConsumer && (await stopConsumer());
        await stopProducer();
    };

    if (!DURATION_S) {
        process.once('SIGINT', stop);
        return;
    }

    const durationS = parseInt(DURATION_S);
    await delay(parseInt(WARMUP_S) * 1000);
    isMeasuring = true;
    await delay(durationS * 1000);
    isRunning = false;
    const drainUntil = Date.now() + 10_000;
    while (stopConsumer && totals.consumed < totals.produced && Date.now() < drainUntil) await delay(100);
    isMeasuring = false;

    console.log(
        `RESULT ${JSON.stringify({
            producedPerSecond: Math.round(totals.produced / durationS),
            endToEndLatencyAvgMs: Math.round(average(totals.endToEndLatencies)),
            endToEndLatencyP99Ms: percentile(totals.endToEndLatencies, 99),
            produceLatencyAvgMs: Math.round(average(totals.produceLatencies)),
            produceLatencyP99Ms: percentile(totals.produceLatencies, 99),
        })}`,
    );
    await stop();
    process.exit(0);
};
