import { KafkaTSApiError } from '../../utils/error';

export type EndTxnRequest = {
    transactionalId: string;
    producerId: bigint;
    producerEpoch: number;
    committed: boolean;
};

export type EndTxnResponse = {
    throttleTimeMs: number;
    errorCode: number;
    producerId: bigint;
    producerEpoch: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends EndTxnResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
