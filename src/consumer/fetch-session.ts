export type FetchOffset = { topic: string; partition: number; offset: bigint };

export class FetchSession {
    private id = 0;
    private epoch = 0;
    private sentOffsets: Record<string, bigint> = {};

    public createRequest(offsets: FetchOffset[]) {
        return {
            sessionId: this.id,
            sessionEpoch: this.epoch,
            offsets: offsets.filter(
                ({ topic, partition, offset }) => this.sentOffsets[`${topic}:${partition}`] !== offset,
            ),
        };
    }

    public update(sessionId: number, offsets: FetchOffset[]) {
        if (!sessionId) return this.reset();

        this.id = sessionId;
        this.epoch++;
        offsets.forEach(({ topic, partition, offset }) => (this.sentOffsets[`${topic}:${partition}`] = offset));
    }

    public reset() {
        this.id = 0;
        this.epoch = 0;
        this.sentOffsets = {};
    }
}
