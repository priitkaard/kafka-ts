import { KafkaTSApiError } from '../../utils/error';

export type OffsetDeleteRequest = {
    groupId: string;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
        }[];
    }[];
};

export type OffsetDeleteResponse = {
    errorCode: number;
    throttleTimeMs: number;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
        }[];
    }[];
};

export const throwIfError = <T extends OffsetDeleteResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    result.topics.forEach((topic) => {
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
