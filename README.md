# KafkaTS

**KafkaTS** is a Apache Kafka client library for Node.js. It provides both a low-level API for communicating directly with the Apache Kafka cluster and high-level APIs for publishing and subscribing to Kafka topics.

**Please note that this project is still in early development and is not yet ready for production use. The interface before stable release is subject to change.**

## Installation

```bash
npm install kafka-ts
```

## Quick start

### Create kafka client

```typescript
export const kafka = createKafkaClient({
    clientId: 'my-app',
    bootstrapServers: [{ host: 'localhost', port: 9092 }],
});
```

#### Consuming messages

```typescript
const consumer = await kafka.startConsumer({
    groupId: 'my-consumer-group'.
    topics: ['my-topic'],
    onMessage: (message) => {
        console.log(message);
    },
});
```

#### Producing messages

```typescript
export const producer = kafka.createProducer();

await producer.send([{ topic: 'my-topic', partition: 0, key: 'key', value: 'value' }]);
```

#### Low-level API

```typescript
const cluster = kafka.createCluster();
await cluster.connect();

const { controllerId } = await cluster.sendRequest(API.METADATA, {
    allowTopicAutoCreation: false,
    includeTopicAuthorizedOperations: false,
    topics: [],
});

await cluster.sendRequestToNode(controllerId)(API.CREATE_TOPICS, {
    validateOnly: false,
    timeoutMs: 10_000,
    topics: [
        {
            name: 'my-topic',
            numPartitions: 10,
            replicationFactor: 3,
            assignments: [],
            configs: [],
        },
    ],
});

await cluster.disconnect();
```

#### Graceful shutdown

```typescript
process.once('SIGTERM', async () => {
    await consumer.close(); // waits for the consumer to finish processing the last batch and disconnects
    await producer.close();
});
```

See the [examples](./examples) for more detailed examples.

## Motivation

The existing low-level libraries (e.g. node-rdkafka) are bindings on librdkafka, which doesn't give enough control over the consumer logic.
The existing high-level libraries (e.g. kafkajs) are missing a few crucial features.

### New features compared to kafkajs

-   **Static consumer membership** - Rebalancing during rolling deployments causes delays. Using `groupInstanceId` in addition to `groupId` can avoid rebalancing and continue consuming partitions in the existing assignment.
-   **Consuming messages without consumer groups** - When you don't need the consumer to track the partition offsets, you can simply create a consumer without groupId and always either start consuming messages from the beginning or from the latest partition offset.
-   **Low-level API requests** - It's possible to communicate directly with the Kafka cluster using the kafka api protocol.

## Backlog

Minimal set of features required before a stable release:

-   Partitioner (Currently have to specify the partition on producer.send())
-   API versioning (Currently only tested against Kafka 3.7+)
-   SASL SCRAM-SHA-512 support
