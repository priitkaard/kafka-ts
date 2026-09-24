import { KafkaTSApiError } from '../../utils/error';

export type ShareGroupDescribeRequest = {
    groupIds: string[];
    includeAuthorizedOperations: boolean;
};

export type ShareGroupDescribeResponse = {
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
            rackId: string | null;
            memberEpoch: number;
            clientId: string;
            clientHost: string;
            subscribedTopicNames: string[];
            assignment: {
                topicPartitions: {
                    topicId: string;
                    topicName: string;
                    partitions: number[];
                    tags: Record<number, Buffer>;
                }[];
                tags: Record<number, Buffer>;
            };
            tags: Record<number, Buffer>;
        }[];
        authorizedOperations: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ShareGroupDescribeResponse>(result: T) => {
    result.groups.forEach((group) => {
        if (group.errorCode) throw new KafkaTSApiError(group.errorCode, group.errorMessage, result);
    });
    return result;
};
