import { KafkaTSApiError } from '../../utils/error';

export type DescribeQuorumRequest = {
    topics: {
        topicName: string;
        partitions: {
            partitionIndex: number;
        }[];
    }[];
};

export type DescribeQuorumResponse = {
    errorCode: number;
    errorMessage: string | null;
    topics: {
        topicName: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            errorMessage: string | null;
            leaderId: number;
            leaderEpoch: number;
            highWatermark: bigint;
            currentVoters: {
                replicaId: number;
                replicaDirectoryId: string;
                logEndOffset: bigint;
                lastFetchTimestamp: bigint;
                lastCaughtUpTimestamp: bigint;
                tags: Record<number, Buffer>;
            }[];
            observers: {
                replicaId: number;
                replicaDirectoryId: string;
                logEndOffset: bigint;
                lastFetchTimestamp: bigint;
                lastCaughtUpTimestamp: bigint;
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    nodes: {
        nodeId: number;
        listeners: {
            name: string;
            host: string;
            port: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeQuorumResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    result.topics.forEach((topic) => {
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
