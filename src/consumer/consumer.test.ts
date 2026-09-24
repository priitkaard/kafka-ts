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
        const flush = vi.fn();
        (consumer as any).offsetManager = {
            resolve: () => {},
            isResolved: () => false,
            flush,
        };

        await (consumer as any).process(fetchResponse());

        expect(flush).toHaveBeenCalled();
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
