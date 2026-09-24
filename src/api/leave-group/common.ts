import { KafkaTSApiError } from '../../utils/error';

export type LeaveGroupRequest = {
    groupId: string;
    members: {
        memberId: string;
        groupInstanceId: string | null;
        reason: string | null;
    }[];
};

export type LeaveGroupResponse = {
    throttleTimeMs: number;
    errorCode: number;
    members: {
        memberId: string;
        groupInstanceId: string | null;
        errorCode: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: LeaveGroupResponse) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    result.members.forEach((member) => {
        if (member.errorCode) throw new KafkaTSApiError(member.errorCode, null, result);
    });
    return result;
};
