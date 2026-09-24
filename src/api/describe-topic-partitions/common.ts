import { KafkaTSApiError } from '../../utils/error';

export type DescribeTopicPartitionsRequest = {
    topics: {
        name: string;
    }[];
    responsePartitionLimit: number;
    cursor: {
        topicName: string;
        partitionIndex: number;
    } | null;
};

export type DescribeTopicPartitionsResponse = {
    throttleTimeMs: number;
    topics: {
        errorCode: number;
        name: string | null;
        topicId: string;
        isInternal: boolean;
        partitions: {
            errorCode: number;
            partitionIndex: number;
            leaderId: number;
            leaderEpoch: number;
            replicaNodes: number[];
            isrNodes: number[];
            eligibleLeaderReplicas: number[];
            lastKnownElr: number[];
            offlineReplicas: number[];
            tags: Record<number, Buffer>;
        }[];
        topicAuthorizedOperations: number;
        tags: Record<number, Buffer>;
    }[];
    nextCursor: {
        topicName: string;
        partitionIndex: number;
        tags: Record<number, Buffer>;
    } | null;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeTopicPartitionsResponse>(result: T) => {
    result.topics.forEach((topic) => {
        if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, null, result);
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
