import { API, API_ERROR } from '../api';
import { Cluster } from '../cluster';
import { KafkaTSApiError } from '../utils/error';
import { shared } from '../utils/shared';

const MAX_INT32 = 2 ** 31 - 1;
const NO_PRODUCER_ID = -1n;
const NO_PRODUCER_EPOCH = -1;

const incrementSequence = (sequence: number, increment: number) =>
    sequence > MAX_INT32 - increment ? increment - (MAX_INT32 - sequence) - 1 : sequence + increment;

const PRODUCER_ID_ERROR_CODES: number[] = [
    API_ERROR.OUT_OF_ORDER_SEQUENCE_NUMBER,
    API_ERROR.UNKNOWN_PRODUCER_ID,
    API_ERROR.INVALID_PRODUCER_EPOCH,
    API_ERROR.PRODUCER_FENCED,
];

export const isProducerIdError = (error: unknown) =>
    error instanceof KafkaTSApiError && PRODUCER_ID_ERROR_CODES.includes(error.errorCode);

type ProducerStateOptions = {
    cluster: Cluster;
};

export class ProducerState {
    public producerId = NO_PRODUCER_ID;
    public producerEpoch = NO_PRODUCER_EPOCH;
    public generation = 0;
    private sequences: Record<string, Record<number, number>> = {};

    constructor(private options: ProducerStateOptions) {}

    public initProducerId = shared(async () => {
        const result = await this.options.cluster.sendRequest(API.INIT_PRODUCER_ID, {
            transactionalId: null,
            transactionTimeoutMs: 0,
            producerId: NO_PRODUCER_ID,
            producerEpoch: NO_PRODUCER_EPOCH,
        });
        this.producerId = result.producerId;
        this.producerEpoch = result.producerEpoch;
        this.sequences = {};
        this.generation++;
    });

    public get isInitialized() {
        return this.producerId !== NO_PRODUCER_ID;
    }

    public reset(generation: number) {
        if (generation !== this.generation) return;

        this.producerId = NO_PRODUCER_ID;
        this.producerEpoch = NO_PRODUCER_EPOCH;
        this.sequences = {};
        this.generation++;
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
