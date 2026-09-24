import { KafkaTSApiError } from '../../utils/error';

export type ConsumerGroupHeartbeatRequest = {
    groupId: string;
    memberId: string;
    memberEpoch: number;
    instanceId: string | null;
    rackId: string | null;
    rebalanceTimeoutMs: number;
    subscribedTopicNames: string[] | null;
    subscribedTopicRegex?: string | null;
    serverAssignor: string | null;
    topicPartitions:
        | {
              topicId: string | null;
              partitions: number[];
          }[]
        | null;
};

export type ConsumerGroupHeartbeatResponse = {
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

export const throwIfError = <T extends ConsumerGroupHeartbeatResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
