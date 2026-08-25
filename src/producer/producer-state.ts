import { API } from '../api';
import { Cluster } from '../cluster';

const MAX_INT32 = 2 ** 31 - 1;

const incrementSequence = (sequence: number, increment: number) =>
    sequence > MAX_INT32 - increment ? increment - (MAX_INT32 - sequence) - 1 : sequence + increment;

type ProducerStateOptions = {
    cluster: Cluster;
};

export class ProducerState {
    public producerId = 0n;
    public producerEpoch = 0;
    private sequences: Record<string, Record<number, number>> = {};

    constructor(private options: ProducerStateOptions) {}

    public async initProducerId(): Promise<void> {
        const result = await this.options.cluster.sendRequest(API.INIT_PRODUCER_ID, {
            transactionalId: null,
            transactionTimeoutMs: 0,
            producerId: this.producerId,
            producerEpoch: this.producerEpoch,
        });
        this.producerId = result.producerId;
        this.producerEpoch = result.producerEpoch;
        this.sequences = {};
    }

    public reset() {
        this.producerId = 0n;
        this.producerEpoch = 0;
        this.sequences = {};
    }

    public getSequence(topic: string, partition: number) {
        return this.sequences[topic]?.[partition] ?? 0;
    }

    public updateSequence(topic: string, partition: number, messagesCount: number) {
        this.sequences[topic] ??= {};
        this.sequences[topic][partition] ??= 0;
        this.sequences[topic][partition] = incrementSequence(this.sequences[topic][partition], messagesCount);
    }
}
