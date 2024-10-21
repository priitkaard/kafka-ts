import { kafka } from './client';

(async () => {
    const topic = 'example-topic';

    const producer = kafka.createProducer({ allowTopicAutoCreation: true });
    const consumer = await kafka.startConsumer({
        topics: [topic],
        onBatch: async (messages) => {
            await producer.send(
                messages.map((message) => ({
                    ...message,
                    headers: { 'X-Replicated': 'true' },
                    topic: `${message.topic}-replicated`,
                    offset: 0n,
                })),
            );
            console.log(`Replicated ${messages.length} messages`);
        },
    });
    process.on('SIGINT', async () => {
        await consumer.close();
        await producer.close();
    });
})();
