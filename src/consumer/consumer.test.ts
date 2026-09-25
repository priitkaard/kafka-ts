import { describe, expect, it, vi } from 'vitest';
import { API_ERROR } from '../api';
import { Cluster } from '../cluster';
import { ConnectionError, KafkaTSApiError } from '../utils/error';
import { withTimeout } from '../utils/timeout';
import { Consumer } from './consumer';

const createCluster = () =>
    ({
        requestTimeout: 30_000,
        connect: async () => {},
        disconnect: async () => {},
        sendRequest: async () => {
            throw new Error('Unexpected api request');
        },
        sendRequestToNode: () => async () => {
            throw new Error('Unexpected api request');
        },
    }) as unknown as Cluster;

const fetchResponse = () => ({
    responses: [
        {
            topicName: 'topic',
            partitions: [
                {
                    partitionIndex: 0,
                    abortedTransactions: [],
                    records: [
                        {
                            baseTimestamp: 0n,
                            baseOffset: 0n,
                            lastOffsetDelta: 0,
                            records: [{ key: null, value: null, headers: [], timestampDelta: 0, offsetDelta: 0 }],
                        },
                    ],
                },
            ],
        },
    ],
});

describe('Consumer', () => {
    it('resolves close() on a consumer that was never started', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], onBatch: () => {} });

        await expect(withTimeout(consumer.close(), 1_000, 'close did not resolve')).resolves.toBeUndefined();
    });

    it('resolves a second close()', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], onBatch: () => {} });

        await consumer.close();

        await expect(withTimeout(consumer.close(), 1_000, 'close did not resolve')).resolves.toBeUndefined();
    });

    it('advances offsets for a consumer without a group', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], onBatch: () => {} });
        const markCommitted = vi.fn();
        (consumer as any).offsetManager = {
            resolve: () => {},
            isResolved: () => false,
            getPendingOffsets: () => [],
            markCommitted,
        };

        await (consumer as any).process(fetchResponse());
        await (consumer as any).committing;

        expect(markCommitted).toHaveBeenCalled();
    });

    it('skips control batches and aborted transactions', async () => {
        const batch = (baseOffset: bigint, producerId: bigint, value: string | null, isControlBatch = false) => ({
            baseOffset,
            baseTimestamp: 0n,
            lastOffsetDelta: 0,
            producerId,
            isTransactional: true,
            isControlBatch,
            records: [{ key: null, value, headers: [], timestampDelta: 0, offsetDelta: 0 }],
        });
        const response = {
            responses: [
                {
                    topicName: 'topic',
                    partitions: [
                        {
                            partitionIndex: 0,
                            abortedTransactions: [{ producerId: 1n, firstOffset: 1n }],
                            records: [
                                batch(0n, 2n, 'committed'),
                                batch(1n, 1n, 'aborted'),
                                batch(2n, 1n, null, true),
                                batch(3n, 1n, 'next transaction'),
                                batch(4n, 1n, null, true),
                            ],
                        },
                    ],
                },
            ],
        };
        const values: (string | null)[] = [];
        const consumer = new Consumer(createCluster(), {
            topics: ['topic'],
            onBatch: (messages) => values.push(...messages.map(({ value }) => value)),
        });
        const resolve = vi.fn();
        (consumer as any).offsetManager = {
            resolve,
            isResolved: () => false,
            getPendingOffsets: () => [],
            markCommitted: () => {},
        };

        await (consumer as any).process(response);

        expect(values).toEqual(['committed', 'next transaction']);
        expect(resolve).toHaveBeenLastCalledWith('topic', 0, 5n);
    });

    it('advances past a response that only contains control batches', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], onBatch: () => {} });
        const resolve = vi.fn();
        (consumer as any).offsetManager = {
            resolve,
            getPendingOffsets: () => [],
            markCommitted: () => {},
        };
        const response = fetchResponse();
        Object.assign(response.responses[0].partitions[0].records[0], { isControlBatch: true });

        await (consumer as any).process(response);

        expect(resolve).toHaveBeenCalledExactlyOnceWith('topic', 0, 1n);
    });

    it('stops running when the fetch loop fails while recovering', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], groupId: 'group', onBatch: () => {} });

        (consumer as any).running = true;
        (consumer as any).consumerGroup = {
            join: async () => {
                throw new KafkaTSApiError(API_ERROR.NOT_COORDINATOR, null, {});
            },
            findCoordinator: async () => {
                throw new ConnectionError('Coordinator lookup failed');
            },
            leaveGroup: async () => {},
        };

        await (consumer as any).startFetchManager();

        expect((consumer as any).running).toBe(false);
    });

    it('refreshes metadata without rejoining when a fetch connection fails', async () => {
        const cluster = createCluster();
        cluster.sendRequestToNode = () => async () => {
            throw new ConnectionError('Socket closed unexpectedly');
        };
        const consumer = new Consumer(cluster, { topics: ['topic'], groupId: 'group', onBatch: () => {} });
        const join = vi.fn(async () => {});
        const fetchMetadata = vi.fn(async () => {
            if (fetchMetadata.mock.calls.length === 2) (consumer as any).stopRequested = true;
        });

        (consumer as any).consumerGroup = { join, handleLastHeartbeat: () => {} };
        (consumer as any).metadata = {
            getAssignment: () => ({ topic: [0] }),
            getTopicPartitionLeaderIds: () => ({ topic: { 0: 1 } }),
            getTopicIdByName: () => '',
        };
        (consumer as any).offsetManager = {
            getPosition: () => 0n,
            getPartitionsWithoutOffset: () => ({}),
            fetchOffsets: async () => {},
        };
        (consumer as any).fetchMetadata = fetchMetadata;

        await (consumer as any).runFetchManager();

        expect(join).toHaveBeenCalledOnce();
        expect(fetchMetadata).toHaveBeenCalledTimes(2);
    });

    it('resets only the partitions that are out of range', async () => {
        const outOfRange = {
            responses: [
                {
                    topicName: 'topic',
                    partitions: [
                        { partitionIndex: 0, errorCode: 0 },
                        { partitionIndex: 1, errorCode: API_ERROR.OFFSET_OUT_OF_RANGE },
                    ],
                },
            ],
        };
        const cluster = createCluster();
        let requests = 0;
        cluster.sendRequestToNode = () => async () => {
            if (requests++) return fetchResponse() as never;
            throw new KafkaTSApiError(API_ERROR.OFFSET_OUT_OF_RANGE, null, outOfRange);
        };
        const consumer = new Consumer(cluster, { topics: ['topic'], onBatch: () => {} });
        const fetchOffsets = vi.fn(async () => {});

        (consumer as any).metadata = { getTopicIdByName: () => '' };
        (consumer as any).offsetManager = { getPosition: () => 0n };
        (consumer as any).fetchOffsets = fetchOffsets;

        await (consumer as any).fetch(1, { topic: [0, 1] });

        expect(fetchOffsets).toHaveBeenCalledExactlyOnceWith({ topic: [1] });
    });

    it('rejoins without leaving the group when the coordinator connection fails', async () => {
        const cluster = createCluster();
        cluster.ensureConnected = vi.fn(async () => {});
        const consumer = new Consumer(cluster, { topics: ['topic'], groupId: 'group', onBatch: () => {} });
        const join = vi.fn(async () => {
            if (join.mock.calls.length === 1) throw new ConnectionError('Socket closed unexpectedly');
            (consumer as any).stopRequested = true;
        });
        const findCoordinator = vi.fn(async () => {});
        const leaveGroup = vi.fn(async () => {});

        (consumer as any).consumerGroup = { join, findCoordinator, leaveGroup };

        await (consumer as any).runFetchManager();

        expect(cluster.ensureConnected).toHaveBeenCalledOnce();
        expect(findCoordinator).toHaveBeenCalledOnce();
        expect(join).toHaveBeenCalledTimes(2);
        expect(leaveGroup).not.toHaveBeenCalled();
    });

    it('keeps its membership when the coordinator connection fails after joining', async () => {
        const cluster = createCluster();
        cluster.ensureConnected = vi.fn(async () => {});
        const consumer = new Consumer(cluster, { topics: ['topic'], groupId: 'group', onBatch: () => {} });
        const handleLastHeartbeat = vi.fn(() => {
            if (handleLastHeartbeat.mock.calls.length === 1) throw new ConnectionError('Socket closed unexpectedly');
            (consumer as any).stopRequested = true;
        });
        const join = vi.fn(async () => {});
        const findCoordinator = vi.fn(async () => {});

        (consumer as any).consumerGroup = { join, findCoordinator, handleLastHeartbeat };
        (consumer as any).metadata = { getAssignment: () => ({}), getTopicPartitionLeaderIds: () => ({}) };

        await (consumer as any).runFetchManager();

        expect(findCoordinator).toHaveBeenCalledOnce();
        expect(join).toHaveBeenCalledOnce();
    });

    it('stops fetching as soon as a heartbeat fails', () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], groupId: 'group', onBatch: () => {} });
        const stop = vi.fn(async () => {});
        (consumer as any).fetchManager = { stop };

        consumer.emit('heartbeatError', new ConnectionError('Socket closed unexpectedly'));

        expect(stop).toHaveBeenCalledOnce();
    });

    it.each([API_ERROR.ILLEGAL_GENERATION, API_ERROR.UNKNOWN_MEMBER_ID])(
        'rejoins the group when a heartbeat fails with error code %s',
        async (errorCode) => {
            const consumer = new Consumer(createCluster(), { topics: ['topic'], groupId: 'group', onBatch: () => {} });
            const handleLastHeartbeat = vi.fn(() => {
                if (handleLastHeartbeat.mock.calls.length === 1) throw new KafkaTSApiError(errorCode, null, {});
                (consumer as any).stopRequested = true;
            });
            const join = vi.fn(async () => {});
            const restart = vi.fn(async () => {});

            (consumer as any).consumerGroup = { join, handleLastHeartbeat };
            (consumer as any).metadata = { getAssignment: () => ({}), getTopicPartitionLeaderIds: () => ({}) };
            (consumer as any).restart = restart;

            await (consumer as any).runFetchManager();

            expect(join).toHaveBeenCalledTimes(2);
            expect(restart).not.toHaveBeenCalled();
        },
    );

    it('closes when the group reports the instance id was fenced', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], groupId: 'group', onBatch: () => {} });

        (consumer as any).running = true;
        (consumer as any).consumerGroup = {
            join: async () => {
                throw new KafkaTSApiError(API_ERROR.FENCED_INSTANCE_ID, null, {});
            },
            leaveGroup: async () => {},
        };

        await (consumer as any).startFetchManager();

        expect((consumer as any).closed).toBe(true);
        expect((consumer as any).running).toBe(false);
    });

    it('does not restart internally after close()', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], onBatch: () => {} });

        await consumer.close();

        await expect(withTimeout((consumer as any).restart(), 500, 'restart did not return')).resolves.toBeUndefined();
        expect((consumer as any).running).toBe(false);
    });

    it('does not restart internally after a forced close()', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], onBatch: () => {} });

        await consumer.close(true);

        await expect(withTimeout((consumer as any).restart(), 500, 'restart did not return')).resolves.toBeUndefined();
        expect((consumer as any).running).toBe(false);
    });

    it('starts again after close()', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], onBatch: () => {} });

        await consumer.close();
        expect((consumer as any).closed).toBe(true);

        void consumer.start();
        expect((consumer as any).closed).toBe(false);

        await expect(withTimeout(consumer.close(), 3_000, 'close did not resolve')).resolves.toBeUndefined();
    });

    it('resolves close() while the consumer is joining the group', async () => {
        const consumer = new Consumer(createCluster(), { topics: ['topic'], groupId: 'group', onBatch: () => {} });
        let releaseJoin: () => void = () => {};
        const join = new Promise<void>((resolve) => (releaseJoin = resolve));

        (consumer as any).consumerGroup = {
            init: async () => {},
            join: () => join,
            leaveGroup: async () => {},
        };
        (consumer as any).fetchMetadata = async () => {};
        (consumer as any).fetchOffsets = async () => {};
        (consumer as any).metadata = { setAssignment: () => {}, getTopicPartitions: () => ({}) };

        await consumer.start();
        await new Promise((resolve) => setTimeout(resolve, 10));

        const closing = withTimeout(consumer.close(), 1_000, 'close did not resolve');
        releaseJoin();

        await expect(closing).resolves.toBeUndefined();
    });
});
