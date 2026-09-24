import { KafkaTSApiError } from '../../utils/error';

export type OffsetForLeaderEpochRequest = {
    replicaId?: number;
    topics: {
        topic: string;
        partitions: {
            partition: number;
            currentLeaderEpoch: number;
            leaderEpoch: number;
        }[];
    }[];
};

export type OffsetForLeaderEpochResponse = {
    throttleTimeMs: number;
    topics: {
        topic: string;
        partitions: {
            errorCode: number;
            partition: number;
            leaderEpoch: number;
            endOffset: bigint;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends OffsetForLeaderEpochResponse>(result: T) => {
    result.topics.forEach((topic) => {
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
