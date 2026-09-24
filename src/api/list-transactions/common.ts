import { KafkaTSApiError } from '../../utils/error';

export type ListTransactionsRequest = {
    stateFilters: string[];
    producerIdFilters: bigint[];
    durationFilter?: bigint;
    transactionalIdPattern?: string | null;
};

export type ListTransactionsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    unknownStateFilters: string[];
    transactionStates: {
        transactionalId: string;
        producerId: bigint;
        transactionState: string;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ListTransactionsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
