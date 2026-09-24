import { KafkaTSApiError } from '../../utils/error';

export type RenewDelegationTokenRequest = {
    hmac: Buffer;
    renewPeriodMs: bigint;
};

export type RenewDelegationTokenResponse = {
    errorCode: number;
    expiryTimestampMs: bigint;
    throttleTimeMs: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends RenewDelegationTokenResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
