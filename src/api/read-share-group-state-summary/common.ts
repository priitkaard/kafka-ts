import { KafkaTSApiError } from '../../utils/error';

export type ReadShareGroupStateSummaryRequest = {
    groupId: string;
    topics: {
        topicId: string | null;
        partitions: {
            partition: number;
            leaderEpoch: number;
        }[];
    }[];
};

export type ReadShareGroupStateSummaryResponse = {
    results: {
        topicId: string;
        partitions: {
            partition: number;
            errorCode: number;
            errorMessage: string | null;
            stateEpoch: number;
            leaderEpoch: number;
            startOffset: bigint;
            deliveryCompleteCount: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ReadShareGroupStateSummaryResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        resultItem.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
