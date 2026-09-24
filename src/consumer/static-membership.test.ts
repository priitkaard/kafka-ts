import { randomBytes } from 'crypto';
import { readFileSync } from 'fs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { API } from '../api';
import { saslPlain } from '../auth';
import { createKafkaClient } from '../client';
import { Cluster } from '../cluster';
import { delay } from '../utils/delay';
import { Consumer, ConsumerOptions } from './consumer';

const kafka = createKafkaClient({
    clientId: 'kafka-ts',
    bootstrapServers: [{ host: 'localhost', port: 39092 }],
    sasl: saslPlain({ username: 'admin', password: 'admin' }),
    ssl: { ca: readFileSync('./certs/ca.crt').toString() },
});

const waitFor = async (condition: () => boolean | Promise<boolean>, timeoutMs = 30_000) => {
    const startedAt = Date.now();
    while (!(await condition())) {
        if (Date.now() - startedAt > timeoutMs) throw new Error('Condition was not met in time');
        await delay(100);
    }
};

const getGroupState = (consumer: Consumer) => {
    const { consumerGroup, metadata, closed } = consumer as any;
    return { generationId: consumerGroup.generationId, assignment: metadata.getAssignment(), closed };
};

const hasJoined = (consumer: Consumer) => getGroupState(consumer).generationId > 0;

describe.sequential('Static membership', () => {
    const topic = `kafka-ts-static-${randomBytes(6).toString('hex')}`;
    const groupId = `kafka-ts-static-${randomBytes(6).toString('hex')}`;
    const consumers: Consumer[] = [];
    let cluster: Cluster;

    const startConsumer = async (groupInstanceId: string, onBatch: ConsumerOptions['onBatch'] = () => {}) => {
        const consumer = await kafka.startConsumer({
            topics: [topic],
            groupId,
            groupInstanceId,
            sessionTimeoutMs: 30_000,
            fromBeginning: true,
            onBatch,
        });
        consumers.push(consumer);
        return consumer;
    };

    beforeAll(async () => {
        cluster = kafka.createCluster();
        await cluster.connect();
        await cluster.sendRequest(API.CREATE_TOPICS, {
            topics: [{ name: topic, numPartitions: 2, replicationFactor: 3 }],
        });
        await waitFor(() =>
            cluster
                .sendRequest(API.METADATA, { topics: [{ id: null, name: topic }] })
                .then(() => true)
                .catch(() => false),
        );
    });

    afterAll(async () => {
        await Promise.all(consumers.map((consumer) => consumer.close(true)));
        await cluster.sendRequest(API.DELETE_TOPICS, { topics: [{ name: topic, topicId: null }] });
        await cluster.disconnect();
    });

    it('replaces an instance without rebalancing the rest of the group', async () => {
        const first = await startConsumer('instance-a');
        const second = await startConsumer('instance-b');
        await waitFor(() => {
            const [a, b] = [getGroupState(first), getGroupState(second)];
            return hasJoined(first) && a.generationId === b.generationId && a.assignment[topic]?.length === 1;
        });
        const before = getGroupState(first);
        const replacedAssignment = getGroupState(second).assignment;

        let rebalances = 0;
        first.on('rebalanceInProgress', () => rebalances++);

        await second.close();
        const messages: string[] = [];
        const replacement = await startConsumer('instance-b', (batch) => {
            messages.push(...batch.map(({ value }) => value!.toString()));
        });
        await waitFor(() => hasJoined(replacement));

        expect(getGroupState(replacement)).toMatchObject({
            generationId: before.generationId,
            assignment: replacedAssignment,
        });

        const producer = kafka.createProducer({});
        await producer.send([{ topic, partition: replacedAssignment[topic][0], value: 'after-swap' }]);
        await producer.close();
        await waitFor(() => messages.includes('after-swap'));

        await delay(6_000);
        expect(rebalances).toBe(0);
        expect(getGroupState(first)).toMatchObject({
            generationId: before.generationId,
            assignment: before.assignment,
        });
    });

    it('fences the running instance when another one joins with the same instance id', async () => {
        const [first, running] = consumers.filter((consumer) => !getGroupState(consumer).closed);
        const { generationId } = getGroupState(first);

        const replacement = await startConsumer('instance-b');
        await waitFor(() => getGroupState(running).closed);
        await waitFor(() => hasJoined(replacement));

        expect(getGroupState(first).generationId).toBe(generationId);
    });
});
