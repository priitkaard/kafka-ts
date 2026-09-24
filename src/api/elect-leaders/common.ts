import { KafkaTSApiError } from '../../utils/error';

export type ElectLeadersRequest = {
    electionType?: number;
    topicPartitions:
        | {
              topic: string;
              partitions: number[];
          }[]
        | null;
    timeoutMs: number;
};

export type ElectLeadersResponse = {
    throttleTimeMs: number;
    errorCode: number;
    replicaElectionResults: {
        topic: string;
        partitionResult: {
            partitionId: number;
            errorCode: number;
            errorMessage: string | null;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ElectLeadersResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    result.replicaElectionResults.forEach((replicaElectionResult) => {
        replicaElectionResult.partitionResult.forEach((partitionResult) => {
            if (partitionResult.errorCode)
                throw new KafkaTSApiError(partitionResult.errorCode, partitionResult.errorMessage, result);
        });
    });
    return result;
};
