import { KafkaTSApiError } from '../../utils/error';

export type UnregisterBrokerRequest = {
    brokerId: number;
};

export type UnregisterBrokerResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends UnregisterBrokerResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
