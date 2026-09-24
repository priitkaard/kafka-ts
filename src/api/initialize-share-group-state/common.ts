import { KafkaTSApiError } from '../../utils/error';

export type InitializeShareGroupStateRequest = {
    groupId: string;
    topics: {
        topicId: string | null;
        partitions: {
            partition: number;
            stateEpoch: number;
            startOffset: bigint;
        }[];
    }[];
};

export type InitializeShareGroupStateResponse = {
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

export const throwIfError = <T extends InitializeShareGroupStateResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        resultItem.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
