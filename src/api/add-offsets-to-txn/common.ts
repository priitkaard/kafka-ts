import { KafkaTSApiError } from '../../utils/error';

export type AddOffsetsToTxnRequest = {
    transactionalId: string;
    producerId: bigint;
    producerEpoch: number;
    groupId: string;
};

export type AddOffsetsToTxnResponse = {
    throttleTimeMs: number;
    errorCode: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends AddOffsetsToTxnResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
