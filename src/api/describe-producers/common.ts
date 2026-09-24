import { KafkaTSApiError } from '../../utils/error';

export type DescribeProducersRequest = {
    topics: {
        name: string;
        partitionIndexes: number[];
    }[];
};

export type DescribeProducersResponse = {
    throttleTimeMs: number;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            errorMessage: string | null;
            activeProducers: {
                producerId: bigint;
                producerEpoch: number;
                lastSequence: number;
                lastTimestamp: bigint;
                coordinatorEpoch: number;
                currentTxnStartOffset: bigint;
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeProducersResponse>(result: T) => {
    result.topics.forEach((topic) => {
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
