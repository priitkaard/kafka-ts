import { KafkaTSApiError } from '../../utils/error';

export type DescribeDelegationTokenRequest = {
    owners:
        | {
              principalType: string;
              principalName: string;
          }[]
        | null;
};

export type DescribeDelegationTokenResponse = {
    errorCode: number;
    tokens: {
        principalType: string;
        principalName: string;
        tokenRequesterPrincipalType: string;
        tokenRequesterPrincipalName: string;
        issueTimestamp: bigint;
        expiryTimestamp: bigint;
        maxTimestamp: bigint;
        tokenId: string;
        hmac: Buffer;
        renewers: {
            principalType: string;
            principalName: string;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    throttleTimeMs: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeDelegationTokenResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
