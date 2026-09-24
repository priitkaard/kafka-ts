import { KafkaTSApiError } from '../../utils/error';

export type DescribeShareGroupOffsetsRequest = {
    groups: {
        groupId: string;
        topics:
            | {
                  topicName: string;
                  partitions: number[];
              }[]
            | null;
    }[];
};

export type DescribeShareGroupOffsetsResponse = {
    throttleTimeMs: number;
    groups: {
        groupId: string;
        topics: {
            topicName: string;
            topicId: string;
            partitions: {
                partitionIndex: number;
                startOffset: bigint;
                leaderEpoch: number;
                lag: bigint;
                errorCode: number;
                errorMessage: string | null;
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        }[];
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeShareGroupOffsetsResponse>(result: T) => {
    result.groups.forEach((group) => {
        if (group.errorCode) throw new KafkaTSApiError(group.errorCode, group.errorMessage, result);
        group.topics.forEach((topic) => {
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
            });
        });
    });
    return result;
};
