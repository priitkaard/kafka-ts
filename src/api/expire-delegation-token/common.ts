import { KafkaTSApiError } from '../../utils/error';

export type ExpireDelegationTokenRequest = {
    hmac: Buffer;
    expiryTimePeriodMs: bigint;
};

export type ExpireDelegationTokenResponse = {
    errorCode: number;
    expiryTimestampMs: bigint;
    throttleTimeMs: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ExpireDelegationTokenResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
