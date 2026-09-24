import { KafkaTSApiError } from '../../utils/error';

export type InitProducerIdRequest = {
    transactionalId: string | null;
    transactionTimeoutMs: number;
    producerId: bigint;
    producerEpoch: number;
    enable2Pc?: boolean;
    keepPreparedTxn?: boolean;
};

export type InitProducerIdResponse = {
    throttleTimeMs: number;
    errorCode: number;
    producerId: bigint;
    producerEpoch: number;
    ongoingTxnProducerId: bigint;
    ongoingTxnProducerEpoch: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: InitProducerIdResponse) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
