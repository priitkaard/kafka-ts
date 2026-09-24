import { KafkaTSApiError } from '../../utils/error';

export type WriteShareGroupStateRequest = {
    groupId: string;
    topics: {
        topicId: string | null;
        partitions: {
            partition: number;
            stateEpoch: number;
            leaderEpoch: number;
            startOffset: bigint;
            deliveryCompleteCount?: number;
            stateBatches: {
                firstOffset: bigint;
                lastOffset: bigint;
                deliveryState: number;
                deliveryCount: number;
            }[];
        }[];
    }[];
};

export type WriteShareGroupStateResponse = {
    results: {
        topicId: string;
        partitions: {
            partition: number;
            errorCode: number;
            errorMessage: string | null;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends WriteShareGroupStateResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        resultItem.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
