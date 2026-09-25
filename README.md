# KafkaTS

**KafkaTS** is a Apache Kafka client library for Node.js. It provides both a low-level API for communicating directly with the Apache Kafka cluster and high-level APIs for publishing and subscribing to Kafka topics.

**Tested Kafka versions:** 3.7.2, 3.8.1, 3.9.1, 4.0.1, 4.1.1, 4.2.0, 4.3.0

Every request is sent with the highest protocol version supported by both KafkaTS and the broker, so older and newer brokers work as long as they support the oldest API versions listed in the [Kafka protocol specification](https://kafka.apache.org/protocol). See [Supported APIs](#supported-apis).

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
| groupProtocol          | 'classic' \| 'consumer'                | false    | 'classic'                       | Group protocol. `'consumer'` uses the incremental rebalance protocol (Kafka 4.0+)    |
| rackId                 | string                                 | false    | _null_                          | Rack id                                                                              |
| isolationLevel         | IsolationLevel                         | false    | IsolationLevel.READ_UNCOMMITTED | Isolation level                                                                      |
| sessionTimeoutMs       | number                                 | false    | 30000                           | Session timeout in milliseconds (classic protocol only)                              |
| rebalanceTimeoutMs     | number                                 | false    | 60000                           | Rebalance timeout in milliseconds                                                    |
| maxWaitMs              | number                                 | false    | 5000                            | Fetch long poll timeout in milliseconds. Must be lower than `requestTimeout`.        |
| minBytes               | number                                 | false    | 1                               | Minimum number of bytes to wait for before returning a fetch response                |
| maxBytes               | number                                 | false    | 52_428_800                      | Maximum number of bytes to return in the fetch response                              |
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

## Supported APIs

All client-facing APIs of the [Kafka protocol](https://kafka.apache.org/protocol) are implemented in every version listed in the protocol specification, and are available through the low-level API (`cluster.sendRequest(API.<CONSTANT>, request)`). Each request uses the highest version supported by both KafkaTS and the broker.

| Key | API                          | Constant                              | Versions | Supported |
| --- | ---------------------------- | ------------------------------------- | -------- | :-------: |
| 0   | Produce                      | `API.PRODUCE`                         | 3–13     |    ✅     |
| 1   | Fetch                        | `API.FETCH`                           | 4–18     |    ✅     |
| 2   | ListOffsets                  | `API.LIST_OFFSETS`                    | 1–11     |    ✅     |
| 3   | Metadata                     | `API.METADATA`                        | 0–13     |    ✅     |
| 8   | OffsetCommit                 | `API.OFFSET_COMMIT`                   | 2–10     |    ✅     |
| 9   | OffsetFetch                  | `API.OFFSET_FETCH`                    | 1–10     |    ✅     |
| 10  | FindCoordinator              | `API.FIND_COORDINATOR`                | 0–6      |    ✅     |
| 11  | JoinGroup                    | `API.JOIN_GROUP`                      | 0–9      |    ✅     |
| 12  | Heartbeat                    | `API.HEARTBEAT`                       | 0–4      |    ✅     |
| 13  | LeaveGroup                   | `API.LEAVE_GROUP`                     | 0–5      |    ✅     |
| 14  | SyncGroup                    | `API.SYNC_GROUP`                      | 0–5      |    ✅     |
| 15  | DescribeGroups               | `API.DESCRIBE_GROUPS`                 | 0–6      |    ✅     |
| 16  | ListGroups                   | `API.LIST_GROUPS`                     | 0–5      |    ✅     |
| 17  | SaslHandshake                | `API.SASL_HANDSHAKE`                  | 0–1      |    ✅     |
| 18  | ApiVersions                  | `API.API_VERSIONS`                    | 0–4      |    ✅     |
| 19  | CreateTopics                 | `API.CREATE_TOPICS`                   | 2–7      |    ✅     |
| 20  | DeleteTopics                 | `API.DELETE_TOPICS`                   | 1–6      |    ✅     |
| 21  | DeleteRecords                | `API.DELETE_RECORDS`                  | 0–2      |    ✅     |
| 22  | InitProducerId               | `API.INIT_PRODUCER_ID`                | 0–6      |    ✅     |
| 23  | OffsetForLeaderEpoch         | `API.OFFSET_FOR_LEADER_EPOCH`         | 2–4      |    ✅     |
| 24  | AddPartitionsToTxn           | `API.ADD_PARTITIONS_TO_TXN`           | 0–5      |    ✅     |
| 25  | AddOffsetsToTxn              | `API.ADD_OFFSETS_TO_TXN`              | 0–4      |    ✅     |
| 26  | EndTxn                       | `API.END_TXN`                         | 0–5      |    ✅     |
| 27  | WriteTxnMarkers              | `API.WRITE_TXN_MARKERS`               | 1–2      |    ✅     |
| 28  | TxnOffsetCommit              | `API.TXN_OFFSET_COMMIT`               | 0–5      |    ✅     |
| 29  | DescribeAcls                 | `API.DESCRIBE_ACLS`                   | 1–3      |    ✅     |
| 30  | CreateAcls                   | `API.CREATE_ACLS`                     | 1–3      |    ✅     |
| 31  | DeleteAcls                   | `API.DELETE_ACLS`                     | 1–3      |    ✅     |
| 32  | DescribeConfigs              | `API.DESCRIBE_CONFIGS`                | 1–4      |    ✅     |
| 33  | AlterConfigs                 | `API.ALTER_CONFIGS`                   | 0–2      |    ✅     |
| 34  | AlterReplicaLogDirs          | `API.ALTER_REPLICA_LOG_DIRS`          | 1–2      |    ✅     |
| 35  | DescribeLogDirs              | `API.DESCRIBE_LOG_DIRS`               | 1–5      |    ✅     |
| 36  | SaslAuthenticate             | `API.SASL_AUTHENTICATE`               | 0–2      |    ✅     |
| 37  | CreatePartitions             | `API.CREATE_PARTITIONS`               | 0–3      |    ✅     |
| 38  | CreateDelegationToken        | `API.CREATE_DELEGATION_TOKEN`         | 1–3      |    ✅     |
| 39  | RenewDelegationToken         | `API.RENEW_DELEGATION_TOKEN`          | 1–2      |    ✅     |
| 40  | ExpireDelegationToken        | `API.EXPIRE_DELEGATION_TOKEN`         | 1–2      |    ✅     |
| 41  | DescribeDelegationToken      | `API.DESCRIBE_DELEGATION_TOKEN`       | 1–3      |    ✅     |
| 42  | DeleteGroups                 | `API.DELETE_GROUPS`                   | 0–2      |    ✅     |
| 43  | ElectLeaders                 | `API.ELECT_LEADERS`                   | 0–2      |    ✅     |
| 44  | IncrementalAlterConfigs      | `API.INCREMENTAL_ALTER_CONFIGS`       | 0–1      |    ✅     |
| 45  | AlterPartitionReassignments  | `API.ALTER_PARTITION_REASSIGNMENTS`   | 0–1      |    ✅     |
| 46  | ListPartitionReassignments   | `API.LIST_PARTITION_REASSIGNMENTS`    | 0        |    ✅     |
| 47  | OffsetDelete                 | `API.OFFSET_DELETE`                   | 0        |    ✅     |
| 48  | DescribeClientQuotas         | `API.DESCRIBE_CLIENT_QUOTAS`          | 0–1      |    ✅     |
| 49  | AlterClientQuotas            | `API.ALTER_CLIENT_QUOTAS`             | 0–1      |    ✅     |
| 50  | DescribeUserScramCredentials | `API.DESCRIBE_USER_SCRAM_CREDENTIALS` | 0        |    ✅     |
| 51  | AlterUserScramCredentials    | `API.ALTER_USER_SCRAM_CREDENTIALS`    | 0        |    ✅     |
| 55  | DescribeQuorum               | `API.DESCRIBE_QUORUM`                 | 0–2      |    ✅     |
| 57  | UpdateFeatures               | `API.UPDATE_FEATURES`                 | 0–2      |    ✅     |
| 60  | DescribeCluster              | `API.DESCRIBE_CLUSTER`                | 0–2      |    ✅     |
| 61  | DescribeProducers            | `API.DESCRIBE_PRODUCERS`              | 0        |    ✅     |
| 64  | UnregisterBroker             | `API.UNREGISTER_BROKER`               | 0        |    ✅     |
| 65  | DescribeTransactions         | `API.DESCRIBE_TRANSACTIONS`           | 0        |    ✅     |
| 66  | ListTransactions             | `API.LIST_TRANSACTIONS`               | 0–2      |    ✅     |
| 68  | ConsumerGroupHeartbeat       | `API.CONSUMER_GROUP_HEARTBEAT`        | 0–1      |    ✅     |
| 69  | ConsumerGroupDescribe        | `API.CONSUMER_GROUP_DESCRIBE`         | 0–1      |    ✅     |
| 71  | GetTelemetrySubscriptions    | `API.GET_TELEMETRY_SUBSCRIPTIONS`     | 0        |    ✅     |
| 72  | PushTelemetry                | `API.PUSH_TELEMETRY`                  | 0        |    ✅     |
| 74  | ListConfigResources          | `API.LIST_CONFIG_RESOURCES`           | 0–1      |    ✅     |
| 75  | DescribeTopicPartitions      | `API.DESCRIBE_TOPIC_PARTITIONS`       | 0        |    ✅     |
| 76  | ShareGroupHeartbeat          | `API.SHARE_GROUP_HEARTBEAT`           | 1        |    ✅     |
| 77  | ShareGroupDescribe           | `API.SHARE_GROUP_DESCRIBE`            | 1        |    ✅     |
| 78  | ShareFetch                   | `API.SHARE_FETCH`                     | 1–2      |    ✅     |
| 79  | ShareAcknowledge             | `API.SHARE_ACKNOWLEDGE`               | 1–2      |    ✅     |
| 80  | AddRaftVoter                 | `API.ADD_RAFT_VOTER`                  | 0–1      |    ✅     |
| 81  | RemoveRaftVoter              | `API.REMOVE_RAFT_VOTER`               | 0        |    ✅     |
| 83  | InitializeShareGroupState    | `API.INITIALIZE_SHARE_GROUP_STATE`    | 0        |    ✅     |
| 84  | ReadShareGroupState          | `API.READ_SHARE_GROUP_STATE`          | 0        |    ✅     |
| 85  | WriteShareGroupState         | `API.WRITE_SHARE_GROUP_STATE`         | 0–1      |    ✅     |
| 86  | DeleteShareGroupState        | `API.DELETE_SHARE_GROUP_STATE`        | 0        |    ✅     |
| 87  | ReadShareGroupStateSummary   | `API.READ_SHARE_GROUP_STATE_SUMMARY`  | 0–1      |    ✅     |
| 88  | StreamsGroupHeartbeat        | `API.STREAMS_GROUP_HEARTBEAT`         | 0        |    ✅     |
| 89  | StreamsGroupDescribe         | `API.STREAMS_GROUP_DESCRIBE`          | 0        |    ✅     |
| 90  | DescribeShareGroupOffsets    | `API.DESCRIBE_SHARE_GROUP_OFFSETS`    | 0–1      |    ✅     |
| 91  | AlterShareGroupOffsets       | `API.ALTER_SHARE_GROUP_OFFSETS`       | 0        |    ✅     |
| 92  | DeleteShareGroupOffsets      | `API.DELETE_SHARE_GROUP_OFFSETS`      | 0        |    ✅     |
