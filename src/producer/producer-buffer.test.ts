import { describe, expect, it, vi } from 'vitest';
import { Cluster } from '../cluster';
import { Message } from '../types';
import { ProducerBuffer } from './producer-buffer';
import { ProducerState } from './producer-state';

const createBuffer = (sendRequest: () => Promise<unknown>) => {
    const cluster = { sendRequestToNode: () => sendRequest } as unknown as Cluster;
    return new ProducerBuffer({
        nodeId: 1,
        maxBatchSize: 500,
        cluster,
        state: new ProducerState({ cluster }),
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

    it('rejects pending enqueues and stays usable when flushing throws unexpectedly', async () => {
        const buffer = createBuffer(() => Promise.resolve({}));

        vi.spyOn(buffer as any, 'compactBuffer').mockImplementationOnce(() => {
            throw new Error('internal');
        });

        await expect(buffer.enqueue(createMessages(1))).rejects.toThrow('internal');
        await expect(buffer.enqueue(createMessages(1))).resolves.toBeUndefined();
    });
});
