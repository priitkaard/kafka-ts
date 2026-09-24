import { KafkaTSApiError } from '../../utils/error';

export type DeleteRecordsRequest = {
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            offset: bigint;
        }[];
    }[];
    timeoutMs: number;
};

export type DeleteRecordsResponse = {
    throttleTimeMs: number;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            lowWatermark: bigint;
            errorCode: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DeleteRecordsResponse>(result: T) => {
    result.topics.forEach((topic) => {
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
