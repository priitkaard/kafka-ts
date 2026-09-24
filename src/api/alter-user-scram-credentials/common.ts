import { KafkaTSApiError } from '../../utils/error';

export type AlterUserScramCredentialsRequest = {
    deletions: {
        name: string;
        mechanism: number;
    }[];
    upsertions: {
        name: string;
        mechanism: number;
        iterations: number;
        salt: Buffer;
        saltedPassword: Buffer;
    }[];
};

export type AlterUserScramCredentialsResponse = {
    throttleTimeMs: number;
    results: {
        user: string;
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends AlterUserScramCredentialsResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        if (resultItem.errorCode) throw new KafkaTSApiError(resultItem.errorCode, resultItem.errorMessage, result);
    });
    return result;
};
