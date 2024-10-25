
export const startBenchmarker = async ({
    createTopic,
    connectProducer,
    startConsumer,
    produce,
}: {
    createTopic: () => Promise<void>;
    connectProducer: () => Promise<() => unknown>;
    startConsumer: (opts: { concurrency: number }, callback: (timestamp: number) => void) => Promise<() => unknown>;
    produce: (opts: { length: number; timestamp: number; acks: -1 | 1 }) => Promise<void>;
}) => {
    await createTopic();

    let consumeCount = 0;
    let consumeLatencySum = 0;
    let produceCount = 0;
    let produceLatencySum = 0;

    const stopProducer = await connectProducer();

    const stopConsumer = await startConsumer({ concurrency: 10 }, (timestamp) => {
        consumeCount++;
        consumeLatencySum += Date.now() - timestamp;
    });

    const interval = setInterval(() => {
        console.log(
            `Consumer latency: ${(consumeLatencySum / consumeCount).toFixed(2)}ms, throughput: ${consumeCount} msg/s | Producer latency: ${(produceLatencySum / produceCount).toFixed(2)}ms, throughput: ${produceCount} msg/s`,
        );
        consumeCount = 0;
        consumeLatencySum = 0;
        produceCount = 0;
        produceLatencySum = 0;
    }, 1000);

    let isRunning = true;
    const produceLoop = async () => {
        if (!isRunning) return;
        const start = Date.now();
        await produce({ length: 10, timestamp: Date.now(), acks: -1 });
        produceCount += 10;
        produceLatencySum += Date.now() - start;
        produceLoop();
    };
    produceLoop();

    process.once('SIGINT', async () => {
        isRunning = false;
        await stopConsumer();
        await stopProducer();
        clearInterval(interval);
    });
};
