import { kafka } from './client';

(async () => {
    const consumer = await kafka.startConsumer({
        groupId: 'example-group',
        groupInstanceId: 'example-group-instance',
        topics: ['my-topic'],
        allowTopicAutoCreation: true,
        onBatch: (batch) => {
            console.log(batch.map(message => ({ ...message, value: message.value?.toString() })));
        },
        batchGranularity: 'broker',
        concurrency: 1,
    });

    process.on('SIGINT', async () => {
        await consumer.close();
    });
})();
