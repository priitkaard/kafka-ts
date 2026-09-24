import { KafkaTSApiError } from '../../utils/error';

export type ReadShareGroupStateRequest = {
    groupId: string;
    topics: {
        topicId: string | null;
        partitions: {
            partition: number;
            leaderEpoch: number;
        }[];
    }[];
};

export type ReadShareGroupStateResponse = {
    results: {
        topicId: string;
        partitions: {
            partition: number;
            errorCode: number;
            errorMessage: string | null;
            stateEpoch: number;
            startOffset: bigint;
            stateBatches: {
                firstOffset: bigint;
                lastOffset: bigint;
                deliveryState: number;
                deliveryCount: number;
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ReadShareGroupStateResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        resultItem.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
