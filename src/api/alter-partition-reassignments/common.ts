import { KafkaTSApiError } from '../../utils/error';

export type AlterPartitionReassignmentsRequest = {
    timeoutMs: number;
    allowReplicationFactorChange?: boolean;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            replicas: number[] | null;
        }[];
    }[];
};

export type AlterPartitionReassignmentsResponse = {
    throttleTimeMs: number;
    allowReplicationFactorChange: boolean;
    errorCode: number;
    errorMessage: string | null;
    responses: {
        name: string;
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

export const throwIfError = <T extends AlterPartitionReassignmentsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    result.responses.forEach((response) => {
        response.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
