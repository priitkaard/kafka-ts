import { API } from '../api';
import { Cluster } from '../cluster';

type ProducerStateOptions = {
    cluster: Cluster;
};

export class ProducerState {
    public producerId = 0n;
    private producerEpoch = 0;
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

    public getSequence(topic: string, partition: number) {
        return this.sequences[topic]?.[partition] ?? 0;
    }

    public updateSequence(topic: string, partition: number, messagesCount: number) {
        this.sequences[topic] ??= {};
        this.sequences[topic][partition] ??= 0;
        this.sequences[topic][partition] += messagesCount;
    }
}
