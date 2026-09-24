import { KafkaTSApiError } from '../../utils/error';

export type HeartbeatRequest = {
    groupId: string;
    generationId: number;
    memberId: string;
    groupInstanceId: string | null;
};

export type HeartbeatResponse = {
    throttleTimeMs: number;
    errorCode: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: HeartbeatResponse) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
