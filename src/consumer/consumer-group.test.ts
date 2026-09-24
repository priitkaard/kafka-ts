import { afterEach, describe, expect, it, vi } from 'vitest';
import { API_ERROR } from '../api';
import { KafkaTSApiError, KafkaTSError } from '../utils/error';
import { ConsumerGroup } from './consumer-group';

const createGroup = (sessionTimeoutMs = 30_000) => {
    const emit = vi.fn();
    const group = new ConsumerGroup({ consumer: { emit }, sessionTimeoutMs } as never);
    const resolvers: (() => void)[] = [];
    const rejecters: ((error: Error) => void)[] = [];
    const heartbeat = vi.fn(
        () =>
            new Promise<void>((resolve, reject) => {
                resolvers.push(resolve);
                rejecters.push(reject);
            }),
    );

    (group as any).heartbeat = heartbeat;
    return {
        group,
        emit,
        heartbeat,
        resolvers,
        rejecters,
        start: () => (group as any).startHeartbeater(),
        stop: () => (group as any).stopHeartbeater(),
    };
};

describe('ConsumerGroup heartbeater', () => {
    afterEach(() => vi.useRealTimers());

    it('does not start a second heartbeat while one is in flight', async () => {
        vi.useFakeTimers();
        const { heartbeat, start, stop } = createGroup();

        start();
        await vi.advanceTimersByTimeAsync(14_000);

        expect(heartbeat).toHaveBeenCalledTimes(1);
        stop();
    });

    it('reports a rebalance exactly once', async () => {
        vi.useFakeTimers();
        const { emit, rejecters, start, stop } = createGroup();

        start();
        await vi.advanceTimersByTimeAsync(5_000);

        rejecters[0](new KafkaTSApiError(API_ERROR.REBALANCE_IN_PROGRESS, null, {}));
        await vi.advanceTimersByTimeAsync(0);

        expect(emit).toHaveBeenCalledExactlyOnceWith('rebalanceInProgress');
        stop();
    });

    it('keeps a rebalance error when a later heartbeat succeeds', async () => {
        vi.useFakeTimers();
        const { group, resolvers, rejecters, start, stop } = createGroup();

        start();
        await vi.advanceTimersByTimeAsync(5_000);
        rejecters[0](new KafkaTSApiError(API_ERROR.REBALANCE_IN_PROGRESS, null, {}));
        await vi.advanceTimersByTimeAsync(5_000);
        resolvers[1]();
        await vi.advanceTimersByTimeAsync(0);

        expect(() => group.handleLastHeartbeat()).toThrow(/REBALANCE_IN_PROGRESS/);

        start();
        expect(() => group.handleLastHeartbeat()).not.toThrow();
        stop();
    });

    it('still reports a rejoin error from a heartbeat the coordinator lookup superseded', async () => {
        vi.useFakeTimers();
        const cluster = {
            sendRequest: async () => ({ coordinators: [{ nodeId: 1 }] }),
            setSeedBroker: async () => {},
        };
        const group = new ConsumerGroup({ consumer: { emit: vi.fn() }, sessionTimeoutMs: 30_000, cluster } as never);
        const rejecters: ((error: Error) => void)[] = [];
        (group as any).heartbeat = () => new Promise<void>((_, reject) => rejecters.push(reject));

        (group as any).startHeartbeater();
        await vi.advanceTimersByTimeAsync(5_000);

        await group.findCoordinator();

        rejecters[0](new KafkaTSApiError(API_ERROR.ILLEGAL_GENERATION, null, {}));
        await vi.advanceTimersByTimeAsync(0);

        expect(() => group.handleLastHeartbeat()).toThrow(/ILLEGAL_GENERATION/);
        (group as any).stopHeartbeater();
    });

    it('ignores a heartbeat left over from a previous generation', async () => {
        vi.useFakeTimers();
        const { group, rejecters, start, stop } = createGroup();

        start();
        await vi.advanceTimersByTimeAsync(5000);
        expect(rejecters).toHaveLength(1);

        start();
        rejecters[0](new KafkaTSError('stale generation'));
        await vi.advanceTimersByTimeAsync(0);

        expect(() => group.handleLastHeartbeat()).not.toThrow();

        await vi.advanceTimersByTimeAsync(5000);
        expect(rejecters).toHaveLength(2);

        rejecters[1](new KafkaTSError('current generation'));
        await vi.advanceTimersByTimeAsync(0);

        expect(() => group.handleLastHeartbeat()).toThrow('current generation');
        stop();
    });
});
