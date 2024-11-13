import { readFileSync } from 'fs';
import { API, createKafkaClient, saslScramSha512 } from 'kafka-ts';
import { startBenchmarker } from './common';

// setTracer(new OpenTelemetryTracer());

const kafka = createKafkaClient({
    bootstrapServers: [{ host: 'localhost', port: 9092 }],
    clientId: 'kafka-ts',
    sasl: saslScramSha512({ username: 'admin', password: 'admin' }),
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});

const producer = kafka.createProducer({ allowTopicAutoCreation: false });

startBenchmarker({
    createTopic: async ({ topic, partitions, replicationFactor }) => {
        const cluster = kafka.createCluster();
        await cluster.connect();

        const { controllerId } = await cluster.sendRequest(API.METADATA, { topics: [] });
        await cluster.setSeedBroker(controllerId);
        await cluster.sendRequest(API.CREATE_TOPICS, {
            topics: [{ name: topic, numPartitions: partitions, replicationFactor }],
        });
        await cluster.disconnect();
    },
    connectProducer: async () => () => producer.close(),
    startConsumer: async ({ groupId, topic, incrementCount }, callback) => {
        const consumer = await kafka.startConsumer({
            groupId,
            topics: [topic],
            onBatch: async (messages) => {
                for (const message of messages) {
                    callback(parseInt(message.timestamp.toString()));
                }
            },
        });
        consumer.on('offsetCommit', () => incrementCount('OFFSET_COMMIT', 1));
        return () => consumer.close();
    },
    produce: async ({ topic, length, timestamp, acks }) => {
        await producer.send(
            Array.from({ length }).map(() => ({
                topic: topic,
                value: 'hello',
                timestamp: BigInt(timestamp),
            })),
            { acks },
        );
    },
});
