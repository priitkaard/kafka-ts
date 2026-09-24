import { describe, expect, it } from 'vitest';
import { Cluster } from '../cluster';
import { ProducerState } from './producer-state';

const createState = () => new ProducerState({ cluster: {} as Cluster });

describe('ProducerState', () => {
    it('tracks sequences per topic partition', () => {
        const state = createState();

        state.updateSequence('topic', 0, 5);
        state.updateSequence('topic', 1, 3);

        expect(state.getSequence('topic', 0)).toBe(5);
        expect(state.getSequence('topic', 1)).toBe(3);
        expect(state.getSequence('other', 0)).toBe(0);
    });

    it('wraps the sequence instead of exceeding the int32 range', () => {
        const state = createState();
        const buffer = Buffer.alloc(4);

        state.updateSequence('topic', 0, 2 ** 31 - 10);
        state.updateSequence('topic', 0, 20);

        const sequence = state.getSequence('topic', 0);
        expect(sequence).toBeGreaterThanOrEqual(0);
        expect(() => buffer.writeInt32BE(sequence)).not.toThrow();
    });

    it('resets sequences together with the producer id', () => {
        const state = createState();

        state.updateSequence('topic', 0, 5);
        state.reset(state.generation);

        expect(state.getSequence('topic', 0)).toBe(0);
        expect(state.isInitialized).toBe(false);
    });

    it('ignores a reset for a producer id that was already replaced', () => {
        const state = createState();
        const generation = state.generation;

        state.reset(generation);
        state.updateSequence('topic', 0, 5);
        state.reset(generation);

        expect(state.getSequence('topic', 0)).toBe(5);
    });

    it('treats producer id 0 as initialized', async () => {
        const cluster = { sendRequest: async () => ({ producerId: 0n, producerEpoch: 0 }) } as unknown as Cluster;
        const state = new ProducerState({ cluster });

        await state.initProducerId();

        expect(state.isInitialized).toBe(true);
    });
});
