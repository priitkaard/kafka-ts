import { KafkaTSApiError } from '../../utils/error';

export type DescribeGroupsRequest = {
    groups: string[];
    includeAuthorizedOperations?: boolean;
};

export type DescribeGroupsResponse = {
    throttleTimeMs: number;
    groups: {
        errorCode: number;
        errorMessage: string | null;
        groupId: string;
        groupState: string;
        protocolType: string;
        protocolData: string;
        members: {
            memberId: string;
            groupInstanceId: string | null;
            clientId: string;
            clientHost: string;
            memberMetadata: Buffer;
            memberAssignment: Buffer;
            tags: Record<number, Buffer>;
        }[];
        authorizedOperations: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeGroupsResponse>(result: T) => {
    result.groups.forEach((group) => {
        if (group.errorCode) throw new KafkaTSApiError(group.errorCode, group.errorMessage, result);
    });
    return result;
};
