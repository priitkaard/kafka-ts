import { KafkaTSApiError } from '../../utils/error';

export type DescribeTransactionsRequest = {
    transactionalIds: string[];
};

export type DescribeTransactionsResponse = {
    throttleTimeMs: number;
    transactionStates: {
        errorCode: number;
        transactionalId: string;
        transactionState: string;
        transactionTimeoutMs: number;
        transactionStartTimeMs: bigint;
        producerId: bigint;
        producerEpoch: number;
        topics: {
            topic: string;
            partitions: number[];
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeTransactionsResponse>(result: T) => {
    result.transactionStates.forEach((transactionState) => {
        if (transactionState.errorCode) throw new KafkaTSApiError(transactionState.errorCode, null, result);
    });
    return result;
};
