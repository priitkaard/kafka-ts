import { delay } from '../utils/delay';

export const startBenchmarker = async ({
    createTopic,
    connectProducer,
    startConsumer,
    produce,
}: {
    createTopic: (opts: { topic: string; partitions: number; replicationFactor: number }) => Promise<void>;
    connectProducer: () => Promise<() => unknown>;
    startConsumer: (
        opts: {
            groupId: string;
            topic: string;
            concurrency: number;
            incrementCount: (key: string, value: number) => void;
        },
        callback: (timestamp: number) => void,
    ) => Promise<() => unknown>;
    produce: (opts: { topic: string; length: number; timestamp: number; acks: -1 | 1 }) => Promise<void>;
}) => {
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
    } = process.env;
    const enableProducer = PRODUCER === 'true';
    const enableConsumer = CONSUMER === 'true';
    const partitions = parseInt(PARTITIONS);
    const replicationFactor = parseInt(REPLICATION_FACTOR);
    const concurrency = parseInt(CONCURRENCY);
    const produceBatchSize = parseInt(PRODUCE_BATCH_SIZE);
    const produceDelayMs = parseInt(PRODUCE_DELAY_MS);

    await createTopic({ topic: TOPIC, partitions, replicationFactor }).catch(console.error);
    await delay(2500);

    let counts: Record<string, number> = {};
    let sums: Record<string, number> = {};

    const incrementCount = (key: string, value: number) => {
        counts[key] = (counts[key] || 0) + value;
    };

    const incrementSum = (key: string, value: number) => {
        sums[key] = (sums[key] || 0) + value;
    };

    const stopProducer = await connectProducer();

    const stopConsumer =
        enableConsumer &&
        (await startConsumer({ groupId: GROUP_ID, topic: TOPIC, concurrency, incrementCount }, (timestamp) => {
            incrementCount('CONSUMER', 1);
            incrementSum('CONSUMER', Date.now() - timestamp);
        }));

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
        await produce({ topic: TOPIC, length: produceBatchSize, timestamp: Date.now(), acks: -1 });
        incrementCount('PRODUCER', 1);
        incrementSum('PRODUCER', Date.now() - start);
        produceDelayMs && (await delay(produceDelayMs));
        produceLoop();
    };
    enableProducer && produceLoop();

    process.once('SIGINT', async () => {
        isRunning = false;
        stopConsumer && (await stopConsumer());
        await stopProducer();
        clearInterval(interval);
    });
};
