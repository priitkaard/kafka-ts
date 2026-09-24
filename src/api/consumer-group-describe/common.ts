import { KafkaTSApiError } from '../../utils/error';

export type ConsumerGroupDescribeRequest = {
    groupIds: string[];
    includeAuthorizedOperations: boolean;
};

export type ConsumerGroupDescribeResponse = {
    throttleTimeMs: number;
    groups: {
        errorCode: number;
        errorMessage: string | null;
        groupId: string;
        groupState: string;
        groupEpoch: number;
        assignmentEpoch: number;
        assignorName: string;
        members: {
            memberId: string;
            instanceId: string | null;
            rackId: string | null;
            memberEpoch: number;
            clientId: string;
            clientHost: string;
            subscribedTopicNames: string[];
            subscribedTopicRegex: string | null;
            assignment: {
                topicPartitions: {
                    topicId: string;
                    topicName: string;
                    partitions: number[];
                    tags: Record<number, Buffer>;
                }[];
                tags: Record<number, Buffer>;
            };
            targetAssignment: {
                topicPartitions: {
                    topicId: string;
                    topicName: string;
                    partitions: number[];
                    tags: Record<number, Buffer>;
                }[];
                tags: Record<number, Buffer>;
            };
            memberType: number;
            tags: Record<number, Buffer>;
        }[];
        authorizedOperations: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ConsumerGroupDescribeResponse>(result: T) => {
    result.groups.forEach((group) => {
        if (group.errorCode) throw new KafkaTSApiError(group.errorCode, group.errorMessage, result);
    });
    return result;
};
