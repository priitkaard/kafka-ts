import { KafkaTSApiError } from '../../utils/error';

export type AddRaftVoterRequest = {
    clusterId: string | null;
    timeoutMs: number;
    voterId: number;
    voterDirectoryId: string | null;
    listeners: {
        name: string;
        host: string;
        port: number;
    }[];
    ackWhenCommitted?: boolean;
};

export type AddRaftVoterResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends AddRaftVoterResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
