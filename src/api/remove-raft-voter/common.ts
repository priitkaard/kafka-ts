import { KafkaTSApiError } from '../../utils/error';

export type RemoveRaftVoterRequest = {
    clusterId: string | null;
    voterId: number;
    voterDirectoryId: string | null;
};

export type RemoveRaftVoterResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends RemoveRaftVoterResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
