import { describe, expect, it, vi } from 'vitest';
import { Cluster } from '../cluster';
import { Metadata } from '../metadata';
import { Message } from '../types';
import { PromiseChain } from '../utils/promise-chain';
import { ProducerBuffer } from './producer-buffer';
import { ProducerState } from './producer-state';

const metadata = { getTopicPartitionLeaderIds: () => ({ topic: { 0: 1 } }) } as unknown as Metadata;

const giveUp = async (error: unknown) => {
    throw error;
};

const createBuffer = (sendRequest: () => Promise<unknown>, initProducerId = vi.fn()) => {
    initProducerId.mockResolvedValue({ producerId: 1n, producerEpoch: 0 });
    const cluster = { sendRequest: initProducerId, sendRequestToNode: () => sendRequest } as unknown as Cluster;
    return new ProducerBuffer({
        maxBatchSize: 500,
        cluster,
        metadata,
        state: new ProducerState({ cluster }),
        partitionLocks: new PromiseChain(),
        retry: giveUp,
    });
};

const createMessages = (count: number): Message[] =>
    Array.from({ length: count }, () => ({ topic: 'topic', partition: 0, value: null }));

describe('ProducerBuffer', () => {
    it('keeps accepting batches after a send failure', async () => {
        const sendRequest = vi.fn().mockRejectedValueOnce(new Error('boom')).mockResolvedValue({});
        const buffer = createBuffer(sendRequest);

        await expect(buffer.enqueue(createMessages(1))).rejects.toThrow('boom');
        await expect(buffer.enqueue(createMessages(1))).resolves.toBeUndefined();
    });

    it('handles a single enqueue larger than the call stack limit', async () => {
        const buffer = createBuffer(() => Promise.resolve({}));

        await expect(buffer.enqueue(createMessages(300_000))).resolves.toBeUndefined();
    });

    it('requests a new producer id after a failed send', async () => {
        const initProducerId = vi.fn();
        const sendRequest = vi.fn().mockRejectedValueOnce(new Error('boom')).mockResolvedValue({});
        const buffer = createBuffer(sendRequest, initProducerId);

        await expect(buffer.enqueue(createMessages(1))).rejects.toThrow('boom');
        await buffer.enqueue(createMessages(1));

        expect(initProducerId).toHaveBeenCalledTimes(2);
    });

    it('sends one batch at a time per partition across buffers', async () => {
        let inFlight = 0;
        let maxInFlight = 0;
        const sendRequest = async () => {
            maxInFlight = Math.max(maxInFlight, ++inFlight);
            await new Promise((resolve) => setTimeout(resolve, 10));
            inFlight--;
            return {};
        };
        const cluster = {
            sendRequest: async () => ({ producerId: 1n, producerEpoch: 0 }),
            sendRequestToNode: () => sendRequest,
        } as unknown as Cluster;
        const options = {
            maxBatchSize: 500,
            cluster,
            metadata,
            state: new ProducerState({ cluster }),
            partitionLocks: new PromiseChain(),
            retry: giveUp,
        };
        const first = new ProducerBuffer(options);
        const second = new ProducerBuffer(options);

        await Promise.all([first.enqueue(createMessages(1)), second.enqueue(createMessages(1))]);

        expect(maxInFlight).toBe(1);
    });
});
