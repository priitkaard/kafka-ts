import { KafkaTSApiError, KafkaTSError } from '../../utils/error';

export type OffsetFetchRequest = {
    groups: {
        groupId: string;
        memberId?: string | null;
        memberEpoch?: number;
        topics: {
            name: string;
            topicId: string;
            partitionIndexes: number[];
        }[];
    }[];
    requireStable: boolean;
};

export type OffsetFetchResponse = {
    throttleTimeMs: number;
    groups: {
        groupId: string;
        topics: (({ name: string } | { topicId: string }) & {
            partitions: {
                partitionIndex: number;
                committedOffset: bigint;
                committedLeaderEpoch: number;
                committedMetadata: string | null;
                errorCode: number;
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        })[];
        errorCode: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const getSingleGroup = ({ groups }: OffsetFetchRequest) => {
    if (groups.length !== 1) throw new KafkaTSError('OffsetFetch before v8 requires exactly 1 group');
    return groups[0];
};

export const throwIfError = <T extends OffsetFetchResponse>(result: T) => {
    result.groups.forEach((group) => {
        if (group.errorCode) throw new KafkaTSApiError(group.errorCode, null, result);
        group.topics.forEach((topic) => {
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
    });
    return result;
};
