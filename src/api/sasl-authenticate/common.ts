import { KafkaTSApiError } from '../../utils/error';

export type SaslAuthenticateRequest = {
    authBytes: Buffer;
};

export type SaslAuthenticateResponse = {
    errorCode: number;
    errorMessage: string | null;
    authBytes: Buffer;
    sessionLifetimeMs: bigint;
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: SaslAuthenticateResponse) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
