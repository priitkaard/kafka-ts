# KafkaTS

**KafkaTS** is a Apache Kafka client library for Node.js. It provides both a low-level API for communicating directly with the Apache Kafka cluster and high-level APIs for publishing and subscribing to Kafka topics.

**Supported Kafka versions:** ^3.6.x, ^4.0.0

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
    groupId: 'my-consumer-group',
    topics: ['my-topic'],
    onBatch: (messages) => {
        console.log(messages);
    },
});
```

#### Producing messages

```typescript
export const producer = kafka.createProducer();

await producer.send([{ topic: 'my-topic', key: 'key', value: 'value' }]);
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

#### Retries

By default KafkaTS retries `onBatch` using an exponential backoff delay up to 5 times (see [src/utils/retrier.ts](./src/utils/retrier.ts)). In case of failure the consumer is restarted.

In case you want to skip failed messages or implement a DLQ-like mechanism, you can overwrite `retrier` on `startConsumer()` and execute your own logic `onFailure`.

Example if you simply want to skip the failing messages:

```typescript
await kafka.startConsumer({
    // ...
    retrier: createExponentialBackoffRetrier({ onFailure: () => {} }),
});
```

#### Static membership

Setting `groupInstanceId` makes the consumer a static group member. A static member doesn't leave the group when it is closed, so it keeps its partitions for `sessionTimeoutMs`. A new consumer that starts with the same `groupInstanceId` within that time takes over the partitions without rebalancing the rest of the group, and a consumer still running with that id is fenced and closes itself. This allows replacing instances one by one (e.g. during a rolling deployment) without stalling the other consumers.

Use a stable, unique `groupInstanceId` per instance (e.g. the pod name of a StatefulSet) and a `sessionTimeoutMs` longer than the time it takes to replace an instance. When an instance is removed for good, its partitions are reassigned after `sessionTimeoutMs`.

```typescript
const consumer = await kafka.startConsumer({
    groupId: 'my-consumer-group',
    groupInstanceId: process.env.HOSTNAME,
    sessionTimeoutMs: 60_000,
    topics: ['my-topic'],
    onBatch: (messages) => console.log(messages),
});
```

#### Partitioning

By default, messages are partitioned by message key or round-robin if the key is null or undefined. Partition can be overwritten by `partition` property in the message. You can also override the default partitioner per producer instance `kafka.createProducer({ partitioner: customPartitioner })`.

**Keyed messages change partition in 1.4.0.** The default partitioner's murmur2 lost precision on the 32-bit multiplications before 1.4.0, so it disagreed with the Java client for most keys. It is now byte-for-byte identical, which means a key can hash to a different partition than it did in 1.3.3 - about 8 in 10 keys move on a 6-partition topic. Both partitioners are live during a rolling upgrade, so per-key ordering is not preserved across it, and keys written before the upgrade stay co-partitioned by the old assignment. If either matters, drain the topic before upgrading, or keep the old assignment by copying the 1.3.3 `murmur2` into a custom `partitioner`.

A simple example how to partition messages by the value in message header `x-partition-key`:

```typescript
import type { Partitioner } from 'kafka-ts';
import { defaultPartitioner } from 'kafka-ts';

const myPartitioner: Partitioner = (context) => {
    const partition = defaultPartitioner(context);
    return (message) => partition({ ...message, key: message.headers?.['x-partition-key'] });
};

const producer = kafka.createProducer({ partitioner: myPartitioner });

await producer.send([{ topic: 'my-topic', value: 'value', headers: { 'x-partition-key': '123' } }]);
```

## Motivation

The existing low-level libraries (e.g. node-rdkafka) are bindings on librdkafka, which doesn't give enough control over the consumer logic.
The existing high-level libraries (e.g. kafkajs) are missing a few crucial features.

### New features compared to kafkajs

- **Static consumer membership** - Rebalancing during rolling deployments causes delays. Using `groupInstanceId` in addition to `groupId` can avoid rebalancing and continue consuming partitions in the existing assignment.
- **Consuming messages without consumer groups** - When you don't need the consumer to track the partition offsets, you can simply create a consumer without groupId and always either start consuming messages from the beginning or from the latest partition offset.
- **Low-level API requests** - It's possible to communicate directly with the Kafka cluster using the kafka api protocol.

## Configuration

### `createKafkaClient()`

| Name             | Type                   | Required | Default | Description                                          |
| ---------------- | ---------------------- | -------- | ------- | ---------------------------------------------------- |
| clientId         | string                 | false    | _null_  | The client id used for all requests.                 |
| bootstrapServers | TcpSocketConnectOpts[] | true     |         | List of kafka brokers for initial cluster discovery. |
| sasl             | SASLProvider           | false    |         | SASL provider                                        |
| ssl              | TLSSocketOptions       | false    |         | SSL configuration.                                   |
| requestTimeout   | number                 | false    | 60000   | Request timeout in milliseconds.                     |
| connectTimeout   | number                 | false    | 10000   | Connect and handshake timeout in milliseconds.       |

#### Supported SASL mechanisms

- PLAIN: `saslPlain({ username, password })`
- SCRAM-SHA-256: `saslScramSha256({ username, password })`
- SCRAM-SHA-512: `saslScramSha512({ username, password })`
- OAUTHBEARER: `oAuthBearer(oAuthAuthenticator({ endpoint, clientId, clientSecret }))`

Custom SASL mechanisms can be implemented following the `SASLProvider` interface. See [src/auth](./src/auth) for examples.

### `kafka.startConsumer()`

| Name                   | Type                                   | Required | Default                         | Description                                                                          |
| ---------------------- | -------------------------------------- | -------- | ------------------------------- | ------------------------------------------------------------------------------------ |
| topics                 | string[]                               | true     |                                 | List of topics to subscribe to                                                       |
| groupId                | string                                 | false    | _null_                          | Consumer group id                                                                    |
| groupInstanceId        | string                                 | false    | _null_                          | Static group member id (see [Static membership](#static-membership))                 |
| rackId                 | string                                 | false    | _null_                          | Rack id                                                                              |
| isolationLevel         | IsolationLevel                         | false    | IsolationLevel.READ_UNCOMMITTED | Isolation level                                                                      |
| sessionTimeoutMs       | number                                 | false    | 30000                           | Session timeout in milliseconds                                                      |
| rebalanceTimeoutMs     | number                                 | false    | 60000                           | Rebalance timeout in milliseconds                                                    |
| maxWaitMs              | number                                 | false    | 5000                            | Fetch long poll timeout in milliseconds. Must be lower than `requestTimeout`.        |
| minBytes               | number                                 | false    | 1                               | Minimum number of bytes to wait for before returning a fetch response                |
| maxBytes               | number                                 | false    | 1_048_576                       | Maximum number of bytes to return in the fetch response                              |
| partitionMaxBytes      | number                                 | false    | 1_048_576                       | Maximum number of bytes to return per partition in the fetch response                |
| allowTopicAutoCreation | boolean                                | false    | false                           | Allow kafka to auto-create topic when it doesn't exist                               |
| fromTimestamp          | bigint                                 | false    | -1                              | Start consuming messages from timestamp (-1 = latest offsets, -2 = earliest offsets) |
| onBatch                | (batch: Message[]) => Promise<unknown> | true     |                                 | Callback executed when a batch of messages is received                               |

### `kafka.createProducer()`

| Name                   | Type        | Required | Default            | Description                                                                             |
| ---------------------- | ----------- | -------- | ------------------ | --------------------------------------------------------------------------------------- |
| allowTopicAutoCreation | boolean     | false    | false              | Allow kafka to auto-create topic when it doesn't exist                                  |
| partitioner            | Partitioner | false    | defaultPartitioner | Custom partitioner function. By default, it uses a default java-compatible partitioner. |
| maxBatchSize           | number      | false    | 500                | Maximum number of messages from separate `send()` calls batched into one request.       |
| maxRetries             | number      | false    | 5                  | Maximum number of retries per `send()` after a recoverable error.                       |
| retryDelayMs           | number      | false    | 100                | Delay before the first retry in milliseconds. Doubled on every subsequent attempt.      |
| maxRetryDelayMs        | number      | false    | 3000               | Upper bound for the exponential retry delay in milliseconds.                            |

Retries resend the same batch with the same producer id and sequence, so the broker discards duplicates. A message may be written twice only if `send()` rejects and is called again.

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
