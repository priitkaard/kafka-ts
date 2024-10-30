# KafkaTS

**KafkaTS** is a Apache Kafka client library for Node.js. It provides both a low-level API for communicating directly with the Apache Kafka cluster and high-level APIs for publishing and subscribing to Kafka topics.

**Supported Kafka versions:** 3.6 and later

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

#### Logging

By default KafkaTS logs out using a JSON logger. This can be globally replaced by calling setLogger method (see [src/utils/logger.ts](./src/utils/logger.ts))

## Motivation

The existing low-level libraries (e.g. node-rdkafka) are bindings on librdkafka, which doesn't give enough control over the consumer logic.
The existing high-level libraries (e.g. kafkajs) are missing a few crucial features.

### New features compared to kafkajs

-   **Static consumer membership** - Rebalancing during rolling deployments causes delays. Using `groupInstanceId` in addition to `groupId` can avoid rebalancing and continue consuming partitions in the existing assignment.
-   **Consuming messages without consumer groups** - When you don't need the consumer to track the partition offsets, you can simply create a consumer without groupId and always either start consuming messages from the beginning or from the latest partition offset.
-   **Low-level API requests** - It's possible to communicate directly with the Kafka cluster using the kafka api protocol.

## Configuration

### `createKafkaClient()`

| Name             | Type                   | Required | Default | Description                                          |
| ---------------- | ---------------------- | -------- | ------- | ---------------------------------------------------- |
| clientId         | string                 | false    | _null_  | The client id used for all requests.                 |
| bootstrapServers | TcpSocketConnectOpts[] | true     |         | List of kafka brokers for initial cluster discovery. |
| sasl             | SASLProvider           | false    |         | SASL provider                                        |
| ssl              | TLSSocketOptions       | false    |         | SSL configuration.                                   |

#### Supported SASL mechanisms

-   PLAIN: `saslPlain({ username, password })`
-   SCRAM-SHA-256: `saslScramSha256({ username, password })`
-   SCRAM-SHA-512: `saslScramSha512({ username, password })`

Custom SASL mechanisms can be implemented following the `SASLProvider` interface. See [src/auth](./src/auth) for examples.

### `kafka.startConsumer()`

| Name                   | Type                                   | Required | Default                         | Description                                                                                                                                                                                                                                                                      |
| ---------------------- | -------------------------------------- | -------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| topics                 | string[]                               | true     |                                 | List of topics to subscribe to                                                                                                                                                                                                                                                   |
| groupId                | string                                 | false    | _null_                          | Consumer group id                                                                                                                                                                                                                                                                |
| groupInstanceId        | string                                 | false    | _null_                          | Consumer group instance id                                                                                                                                                                                                                                                       |
| rackId                 | string                                 | false    | _null_                          | Rack id                                                                                                                                                                                                                                                                          |
| isolationLevel         | IsolationLevel                         | false    | IsolationLevel.READ_UNCOMMITTED | Isolation level                                                                                                                                                                                                                                                                  |
| sessionTimeoutMs       | number                                 | false    | 30000                           | Session timeout in milliseconds                                                                                                                                                                                                                                                  |
| rebalanceTimeoutMs     | number                                 | false    | 60000                           | Rebalance timeout in milliseconds                                                                                                                                                                                                                                                |
| maxWaitMs              | number                                 | false    | 5000                            | Fetch long poll timeout in milliseconds                                                                                                                                                                                                                                          |
| minBytes               | number                                 | false    | 1                               | Minimum number of bytes to wait for before returning a fetch response                                                                                                                                                                                                            |
| maxBytes               | number                                 | false    | 1_048_576                       | Maximum number of bytes to return in the fetch response                                                                                                                                                                                                                          |
| partitionMaxBytes      | number                                 | false    | 1_048_576                       | Maximum number of bytes to return per partition in the fetch response                                                                                                                                                                                                            |
| allowTopicAutoCreation | boolean                                | false    | false                           | Allow kafka to auto-create topic when it doesn't exist                                                                                                                                                                                                                           |
| fromBeginning          | boolean                                | false    | false                           | Start consuming from the beginning of the topic                                                                                                                                                                                                                                  |
| batchGranularity       | BatchGranularity                       | false    | partition                       | Controls messages split from fetch response. Also controls how often offsets are committed. **onBatch** will include messages:<br/>- **partition** - from a single batch<br/>- **topic** - from all topic partitions<br/>- **broker** - from all assignned topics and partitions |
| concurrency            | number                                 | false    | 1                               | How many batches to process concurrently                                                                                                                                                                                                                                         |
| onMessage              | (message: Message) => Promise<unknown> | true     |                                 | Callback executed on every message                                                                                                                                                                                                                                               |
| onBatch                | (batch: Message[]) => Promise<unknown> | true     |                                 | Callback executed on every batch of messages (based on **batchGranuality**)                                                                                                                                                                                                      |

### `kafka.createProducer()`

| Name                   | Type        | Required | Default            | Description                                                                             |
| ---------------------- | ----------- | -------- | ------------------ | --------------------------------------------------------------------------------------- |
| allowTopicAutoCreation | boolean     | false    | false              | Allow kafka to auto-create topic when it doesn't exist                                  |
| partitioner            | Partitioner | false    | defaultPartitioner | Custom partitioner function. By default, it uses a default java-compatible partitioner. |

### `producer.send(messages: Message[])`

<!-- export type Message = {
    topic: string;
    partition?: number;
    timestamp?: bigint;
    key?: Buffer | null;
    value: Buffer | null;
    headers?: Record<string, string>;
}; -->

| Name      | Type                   | Required | Default | Description                                                                                                                |
| --------- | ---------------------- | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------- |
| topic     | string                 | true     |         | Topic to send the message to                                                                                               |
| partition | number                 | false    | _null_  | Partition to send the message to. By default partitioned by key. If key is also missing, partition is assigned round-robin |
| timestamp | bigint                 | false    | _null_  | Message timestamp in milliseconds                                                                                          |
| key       | Buffer \| null         | false    | _null_  | Message key                                                                                                                |
| value     | Buffer \| null         | true     |         | Message value                                                                                                              |
| headers   | Record<string, string> | false    | _null_  | Message headers                                                                                                            |
