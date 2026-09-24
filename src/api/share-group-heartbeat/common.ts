import { KafkaTSApiError } from '../../utils/error';

export type ShareGroupHeartbeatRequest = {
    groupId: string;
    memberId: string;
    memberEpoch: number;
    rackId: string | null;
    subscribedTopicNames: string[] | null;
};

export type ShareGroupHeartbeatResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    memberId: string | null;
    memberEpoch: number;
    heartbeatIntervalMs: number;
    assignment: {
        topicPartitions: {
            topicId: string;
            partitions: number[];
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    } | null;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ShareGroupHeartbeatResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
