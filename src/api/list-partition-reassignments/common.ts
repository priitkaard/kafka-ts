import { KafkaTSApiError } from '../../utils/error';

export type ListPartitionReassignmentsRequest = {
    timeoutMs: number;
    topics:
        | {
              name: string;
              partitionIndexes: number[];
          }[]
        | null;
};

export type ListPartitionReassignmentsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            replicas: number[];
            addingReplicas: number[];
            removingReplicas: number[];
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ListPartitionReassignmentsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
