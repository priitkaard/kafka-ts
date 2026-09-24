import { KafkaTSApiError } from '../../utils/error';

export type AlterShareGroupOffsetsRequest = {
    groupId: string;
    topics: {
        topicName: string;
        partitions: {
            partitionIndex: number;
            startOffset: bigint;
        }[];
    }[];
};

export type AlterShareGroupOffsetsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    responses: {
        topicName: string;
        topicId: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            errorMessage: string | null;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends AlterShareGroupOffsetsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    result.responses.forEach((response) => {
        response.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
