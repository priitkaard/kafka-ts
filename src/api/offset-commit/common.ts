import { KafkaTSApiError } from '../../utils/error';

export type OffsetCommitRequest = {
    groupId: string;
    generationIdOrMemberEpoch: number;
    memberId: string;
    groupInstanceId: string | null;
    topics: {
        name: string;
        topicId: string;
        partitions: {
            partitionIndex: number;
            committedOffset: bigint;
            committedLeaderEpoch: number;
            committedMetadata: string | null;
        }[];
    }[];
};

export type OffsetCommitResponse = {
    throttleTimeMs: number;
    topics: (({ name: string } | { topicId: string }) & {
        partitions: {
            partitionIndex: number;
            errorCode: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    })[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends OffsetCommitResponse>(result: T) => {
    result.topics.forEach((topic) => {
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
