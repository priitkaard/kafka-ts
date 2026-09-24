import { KafkaTSApiError } from '../../utils/error';
import { IsolationLevel } from '../fetch';

export type ListOffsetsRequest = {
    replicaId: number;
    isolationLevel: IsolationLevel;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            currentLeaderEpoch: number;
            timestamp: bigint;
        }[];
    }[];
    timeoutMs?: number;
};

export type ListOffsetsResponse = {
    throttleTimeMs: number;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            timestamp: bigint;
            offset: bigint;
            leaderEpoch: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: ListOffsetsResponse) => {
    result.topics.forEach((topic) => {
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
