import { KafkaJS } from '@confluentinc/kafka-javascript';
import { startBenchmarker } from './common';

const kafka = new KafkaJS.Kafka({
    kafkaJS: {
        brokers: ['localhost:39092'],
        clientId: 'librdkafka',
        ssl: true,
        sasl: { mechanism: 'plain', username: 'admin', password: 'admin' },
        logLevel: KafkaJS.logLevel.NOTHING,
    },
    'ssl.ca.location': '../certs/ca.crt',
});

const producer = kafka.producer({ kafkaJS: { acks: -1, allowAutoTopicCreation: false } });

startBenchmarker({
    createTopic: async ({ topic, partitions, replicationFactor }) => {
        const admin = kafka.admin();
        await admin.connect();
        await admin.createTopics({ topics: [{ topic, numPartitions: partitions, replicationFactor }] });
        await admin.disconnect();
    },
    connectProducer: async () => {
        await producer.connect();
        return () => producer.disconnect();
    },
    startConsumer: async ({ groupId, topic, concurrency, fromBeginning }, callback) => {
        const consumer = kafka.consumer({
            kafkaJS: { groupId, fromBeginning, autoCommit: true },
            'js.consumer.max.batch.size': -1,
        });
        await consumer.connect();
        await consumer.subscribe({ topics: [topic] });
        await consumer.run({
            eachBatch: async ({ batch }) => {
                for (const message of batch.messages) {
                    callback(parseInt(message.timestamp));
                }
            },
            partitionsConsumedConcurrently: concurrency,
        });
        return () => consumer.disconnect();
    },
    produce: async ({ topic, length, value, timestamp }) => {
        await producer.send({
            topic,
            messages: Array.from({ length }).map(() => ({ value, timestamp: timestamp.toString() })),
        });
    },
});
