import { jsonSerializer, log } from 'kafka-ts';
import { kafka } from './client';
import { delay } from '../../dist/utils/delay';

(async () => {
    const consumer = await kafka.startConsumer({
        groupId: 'example-group',
        groupInstanceId: 'example-group-instance',
        topics: ['my-topic'],
        allowTopicAutoCreation: true,
        onBatch: (batch) => {
            log.info(
                `Received batch: ${JSON.stringify(batch.map((message) => ({ ...message, value: message.value?.toString() })), jsonSerializer)}`,
            );
            log.info(`Latency: ${Date.now() - parseInt(batch[0].timestamp.toString())}ms`)
        },
        batchGranularity: 'broker',
        concurrency: 10,
    });

    process.on('SIGINT', async () => {
        await consumer.close();
    });
})();
