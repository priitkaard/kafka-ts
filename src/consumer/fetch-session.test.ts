import { describe, expect, it } from 'vitest';
import { FetchSession } from './fetch-session';

const offsets = (first: bigint, second: bigint) => [
    { topic: 'topic', partition: 0, offset: first },
    { topic: 'topic', partition: 1, offset: second },
];

describe('FetchSession', () => {
    it('starts with a full fetch that creates a session', () => {
        const session = new FetchSession();

        expect(session.createRequest(offsets(0n, 0n))).toEqual({
            sessionId: 0,
            sessionEpoch: 0,
            offsets: offsets(0n, 0n),
        });
    });

    it('only sends partitions whose fetch offset changed', () => {
        const session = new FetchSession();
        session.update(42, offsets(0n, 0n));

        expect(session.createRequest(offsets(10n, 0n))).toEqual({
            sessionId: 42,
            sessionEpoch: 1,
            offsets: [{ topic: 'topic', partition: 0, offset: 10n }],
        });
    });

    it('keeps sending full fetches when the broker does not create a session', () => {
        const session = new FetchSession();
        session.update(0, offsets(0n, 0n));

        expect(session.createRequest(offsets(0n, 0n))).toMatchObject({ sessionId: 0, sessionEpoch: 0 });
        expect(session.createRequest(offsets(0n, 0n)).offsets).toHaveLength(2);
    });

    it('falls back to a full fetch after a reset', () => {
        const session = new FetchSession();
        session.update(42, offsets(0n, 0n));
        session.reset();

        expect(session.createRequest(offsets(0n, 0n))).toEqual({
            sessionId: 0,
            sessionEpoch: 0,
            offsets: offsets(0n, 0n),
        });
    });
});
