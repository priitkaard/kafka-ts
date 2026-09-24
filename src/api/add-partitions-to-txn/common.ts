import { KafkaTSApiError } from '../../utils/error';

export type AddPartitionsToTxnRequest = {
    v3AndBelowTransactionalId?: string;
    v3AndBelowProducerId?: bigint;
    v3AndBelowProducerEpoch?: number;
    v3AndBelowTopics?: {
        name: string;
        partitions: number[];
    }[];
    transactions?: {
        transactionalId: string;
        producerId: bigint;
        producerEpoch: number;
        verifyOnly: boolean;
        topics: {
            name: string;
            partitions: number[];
        }[];
    }[];
};

export type AddPartitionsToTxnResponse = {
    throttleTimeMs: number;
    resultsByTopicV3AndBelow: {
        name: string;
        resultsByPartition: {
            partitionIndex: number;
            partitionErrorCode: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    errorCode: number;
    resultsByTransaction: {
        transactionalId: string;
        topicResults: {
            name: string;
            resultsByPartition: {
                partitionIndex: number;
                partitionErrorCode: number;
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends AddPartitionsToTxnResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    result.resultsByTopicV3AndBelow.forEach((resultsByTopicV3AndBelow) => {
        resultsByTopicV3AndBelow.resultsByPartition.forEach((resultsByPartition) => {
            if (resultsByPartition.partitionErrorCode)
                throw new KafkaTSApiError(resultsByPartition.partitionErrorCode, null, result);
        });
    });
    result.resultsByTransaction.forEach((resultsByTransaction) => {
        resultsByTransaction.topicResults.forEach((topicResult) => {
            topicResult.resultsByPartition.forEach((resultsByPartition) => {
                if (resultsByPartition.partitionErrorCode)
                    throw new KafkaTSApiError(resultsByPartition.partitionErrorCode, null, result);
            });
        });
    });
    return result;
};
