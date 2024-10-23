import { kafka } from './client';

(async () => {
    const consumer = await kafka.startConsumer({
        groupId: 'example-group',
        groupInstanceId: 'example-group-instance',
        topics: ['my-topic'],
        onBatch: (batch) => {
            console.log(batch);
        },
        batchGranularity: 'broker',
        concurrency: 10,
    });

    process.on('SIGINT', async () => {
        await consumer.close();
    });
})();
