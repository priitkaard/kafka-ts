import { readFileSync } from 'fs';
import { API, createKafkaClient, saslScramSha512, setTracer } from 'kafka-ts';
import { OpenTelemetryTracer } from '../utils/opentelemetry';
import { startBenchmarker } from './common';

setTracer(new OpenTelemetryTracer());

const benchmarkId = 'benchmark-kafka-ts';

const kafka = createKafkaClient({
    bootstrapServers: [{ host: 'localhost', port: 9092 }],
    clientId: benchmarkId,
    sasl: saslScramSha512({ username: 'admin', password: 'admin' }),
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});

const producer = kafka.createProducer({ allowTopicAutoCreation: false });

startBenchmarker({
    createTopic: async () => {
        const cluster = kafka.createCluster();
        await cluster.connect();

        const { controllerId } = await cluster.sendRequest(API.METADATA, {
            allowTopicAutoCreation: false,
            includeTopicAuthorizedOperations: false,
            topics: [],
        });
        await cluster.setSeedBroker(controllerId);
        await cluster
            .sendRequest(API.CREATE_TOPICS, {
                validateOnly: false,
                timeoutMs: 10_000,
                topics: [
                    {
                        name: benchmarkId,
                        numPartitions: 10,
                        replicationFactor: 3,
                        assignments: [],
                        configs: [],
                    },
                ],
            })
            .catch(() => {});
        await cluster.disconnect();
    },
    connectProducer: async () => () => producer.close(),
    startConsumer: async ({ concurrency }, callback) => {
        const consumer = await kafka.startConsumer({
            groupId: benchmarkId,
            topics: [benchmarkId],
            onBatch: async (messages) => {
                for (const message of messages) {
                    callback(parseInt(message.timestamp.toString()));
                }
            },
            concurrency,
        });
        return () => consumer.close();
    },
    produce: async ({ length, timestamp, acks }) => {
        await producer.send(
            Array.from({ length }).map(() => ({
                topic: benchmarkId,
                value: Buffer.from('hello'),
                timestamp: BigInt(timestamp),
            })),
            { acks },
        );
    },
});
