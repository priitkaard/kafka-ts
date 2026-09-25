import { readFileSync } from 'fs';
import { Kafka } from 'kafkajs';
import { startBenchmarker } from './common';

const kafkajs = new Kafka({
    brokers: ['localhost:39092'],
    clientId: 'kafkajs',
    sasl: { username: 'admin', password: 'admin', mechanism: 'plain' },
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});

const producer = kafkajs.producer({ allowAutoTopicCreation: false });

startBenchmarker({
    createTopic: async ({ topic, partitions, replicationFactor }) => {
        const admin = kafkajs.admin();
        await admin.connect();
        await admin.createTopics({ topics: [{ topic, numPartitions: partitions, replicationFactor }] });
        await admin.disconnect();
    },
    connectProducer: async () => {
        await producer.connect();
        return () => producer.disconnect();
    },
    startConsumer: async ({ groupId, topic, concurrency, fromBeginning, incrementCount }, callback) => {
        const consumer = kafkajs.consumer({ groupId, allowAutoTopicCreation: false });
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning });
        await consumer.run({
            eachBatch: async ({ batch }) => {
                for (const message of batch.messages) {
                    callback(parseInt(message.timestamp));
                }
            },
            partitionsConsumedConcurrently: concurrency,
            autoCommit: true,
        });
        consumer.on(consumer.events.COMMIT_OFFSETS, () => incrementCount('OFFSET_COMMIT', 1));
        return () => consumer.disconnect();
    },
    produce: async ({ topic, length, value, timestamp, acks }) => {
        await producer.send({
            topic,
            messages: Array.from({ length }).map(() => ({
                value,
                timestamp: timestamp.toString(),
            })),
            acks,
        });
    },
});
