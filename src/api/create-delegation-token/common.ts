import { KafkaTSApiError } from '../../utils/error';

export type CreateDelegationTokenRequest = {
    ownerPrincipalType?: string | null;
    ownerPrincipalName?: string | null;
    renewers: {
        principalType: string;
        principalName: string;
    }[];
    maxLifetimeMs: bigint;
};

export type CreateDelegationTokenResponse = {
    errorCode: number;
    principalType: string;
    principalName: string;
    tokenRequesterPrincipalType: string;
    tokenRequesterPrincipalName: string;
    issueTimestampMs: bigint;
    expiryTimestampMs: bigint;
    maxTimestampMs: bigint;
    tokenId: string;
    hmac: Buffer;
    throttleTimeMs: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends CreateDelegationTokenResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
