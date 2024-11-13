import { jsonSerializer, log } from 'kafka-ts';
import { kafka } from './client';

(async () => {
    const consumer = await kafka.startConsumer({
        groupId: 'example-group',
        groupInstanceId: 'example-group-instance',
        topics: ['my-topic'],
        allowTopicAutoCreation: true,
        onBatch: (batch) => {
            log.info(
                `Received batch: ${JSON.stringify(batch, jsonSerializer)}`,
            );
            log.info(`Latency: ${Date.now() - parseInt(batch[0].timestamp.toString())}ms`)
        },
    });

    process.on('SIGINT', async () => {
        await consumer.close();
    });
})();
