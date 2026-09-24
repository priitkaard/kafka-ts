import { describe, expect, it, vi } from 'vitest';
import { API, API_ERROR } from '../api';
import { Cluster } from '../cluster';
import { Message } from '../types';
import { ConnectionError, KafkaTSApiError } from '../utils/error';
import { Producer } from './producer';

type ProduceRequest = {
    topicData: { partitionData: { producerId: bigint; baseSequence: number }[] }[];
};

const createCluster = (produce: (nodeId: number, request: ProduceRequest) => Promise<unknown>) => {
    const leaderIds = [1];
    let producerId = 0n;

    const cluster = {
        leaderIds,
        ensureConnected: vi.fn(async () => {}),
        disconnect: vi.fn(async () => {}),
        sendRequest: async (api: unknown) => {
            if (api === API.INIT_PRODUCER_ID) return { producerId: producerId++, producerEpoch: 0 };
            if (api === API.METADATA) {
                return {
                    topics: [
                        {
                            name: 'topic',
                            topicId: 'topic-id',
                            partitions: [{ partitionIndex: 0, leaderId: leaderIds[0], isrNodes: [1, 2] }],
                        },
                    ],
                };
            }
            throw new Error('Unexpected api request');
        },
        sendRequestToNode: (nodeId: number) => (_: unknown, request: ProduceRequest) => produce(nodeId, request),
    };

    return cluster as typeof cluster & Cluster;
};

const createMessage = (): Message => ({ topic: 'topic', value: null });

const firstPartitionData = (request: ProduceRequest) => request.topicData[0].partitionData[0];

describe('Producer', () => {
    it('resends the same batch to the new leader after a connection error', async () => {
        const requests: { nodeId: number; request: ProduceRequest }[] = [];
        const cluster = createCluster(async (nodeId, request) => {
            requests.push({ nodeId, request });
            if (requests.length === 2) {
                cluster.leaderIds[0] = 2;
                throw new ConnectionError('Socket closed unexpectedly');
            }
            return {};
        });
        const producer = new Producer(cluster, { retryDelayMs: 1 });

        await producer.send([createMessage()]);
        await producer.send([createMessage()]);
        await producer.send([createMessage()]);

        expect(cluster.disconnect).not.toHaveBeenCalled();
        expect(requests.map(({ nodeId }) => nodeId)).toEqual([1, 1, 2, 2]);
        expect(requests.map(({ request }) => firstPartitionData(request))).toMatchObject([
            { producerId: 0n, baseSequence: 0 },
            { producerId: 0n, baseSequence: 1 },
            { producerId: 0n, baseSequence: 1 },
            { producerId: 0n, baseSequence: 2 },
        ]);
    });

    it('retries with a new producer id after a sequence error', async () => {
        const requests: ProduceRequest[] = [];
        const cluster = createCluster(async (_, request) => {
            requests.push(request);
            if (requests.length === 1) throw new KafkaTSApiError(API_ERROR.OUT_OF_ORDER_SEQUENCE_NUMBER, null, {});
            return {};
        });
        const producer = new Producer(cluster, { retryDelayMs: 1 });

        await expect(producer.send([createMessage()])).resolves.toBeUndefined();
        expect(requests.map(firstPartitionData)).toMatchObject([
            { producerId: 0n, baseSequence: 0 },
            { producerId: 1n, baseSequence: 0 },
        ]);
    });

    it('retries a failed producer id request', async () => {
        const produce = vi.fn(async () => ({}));
        const cluster = createCluster(produce);
        const { sendRequest } = cluster;
        let initAttempts = 0;
        cluster.sendRequest = (async (api: unknown) => {
            if (api === API.INIT_PRODUCER_ID && ++initAttempts === 1) {
                throw new ConnectionError('Socket closed unexpectedly');
            }
            return sendRequest(api);
        }) as typeof cluster.sendRequest;
        const producer = new Producer(cluster, { retryDelayMs: 1 });

        await expect(producer.send([createMessage()])).resolves.toBeUndefined();
        expect(initAttempts).toBe(2);
        expect(produce).toHaveBeenCalledTimes(1);
    });

    it('uses a new producer id once retries are exhausted', async () => {
        const requests: ProduceRequest[] = [];
        const cluster = createCluster(async (_, request) => {
            requests.push(request);
            if (requests.length === 1) throw new ConnectionError('Socket closed unexpectedly');
            return {};
        });
        const producer = new Producer(cluster, { maxRetries: 0 });

        await expect(producer.send([createMessage()])).rejects.toThrow(ConnectionError);
        await producer.send([createMessage()]);

        expect(requests.map(firstPartitionData)).toMatchObject([
            { producerId: 0n, baseSequence: 0 },
            { producerId: 1n, baseSequence: 0 },
        ]);
    });

    it('gives up after the configured number of retries', async () => {
        const produce = vi.fn(async () => {
            throw new ConnectionError('Socket closed unexpectedly');
        });
        const producer = new Producer(createCluster(produce), { maxRetries: 2, retryDelayMs: 1 });

        await expect(producer.send([createMessage()])).rejects.toThrow(ConnectionError);
        expect(produce).toHaveBeenCalledTimes(3);
    });

    it('does not reconnect the cluster when close() races a failing send', async () => {
        let releaseProduce: () => void = () => {};
        const produceGate = new Promise<void>((resolve) => (releaseProduce = resolve));
        const cluster = createCluster(async () => {
            await produceGate;
            throw new ConnectionError('Socket closed unexpectedly');
        });
        const producer = new Producer(cluster, { retryDelayMs: 1 });

        const send = producer.send([createMessage()]);
        await new Promise((resolve) => setTimeout(resolve, 10));
        await producer.close();
        releaseProduce();

        await expect(send).rejects.toThrow(ConnectionError);
        expect(cluster.ensureConnected).toHaveBeenCalledTimes(1);
    });
});
