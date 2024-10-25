import { readFileSync } from 'fs';
import { Kafka } from 'kafkajs';
import { startBenchmarker } from './common';

const benchmarkId = 'benchmark-kafkajs';

const kafkajs = new Kafka({
    brokers: ['localhost:9092'],
    clientId: 'benchmark-kafkajs',
    sasl: { username: 'admin', password: 'admin', mechanism: 'plain' },
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});

const producer = kafkajs.producer({ allowAutoTopicCreation: false });

startBenchmarker({
    createTopic: async () => {
        const admin = kafkajs.admin();
        await admin.connect();
        await admin.createTopics({ topics: [{ topic: benchmarkId, numPartitions: 10, replicationFactor: 3 }] });
        await admin.disconnect();
    },
    connectProducer: async () => {
        await producer.connect();
        return () => producer.disconnect();
    },
    startConsumer: async ({ concurrency }, callback) => {
        const consumer = kafkajs.consumer({ groupId: 'benchmark-kafkajs', allowAutoTopicCreation: false });
        await consumer.connect();
        await consumer.subscribe({ topic: benchmarkId });
        await consumer.run({
            eachBatch: async ({ batch }) => {
                for (const message of batch.messages) {
                    callback(parseInt(message.timestamp));
                }
            },
            partitionsConsumedConcurrently: concurrency,
            autoCommit: true,
        });
        return () => consumer.disconnect();
    },
    produce: async ({ length, timestamp, acks }) => {
        await producer.send({
            topic: 'benchmark-kafkajs',
            messages: Array.from({ length }).map(() => ({
                value: Buffer.from(timestamp.toString()),
                timestamp: timestamp.toString(),
            })),
            acks,
        });
    },
});
