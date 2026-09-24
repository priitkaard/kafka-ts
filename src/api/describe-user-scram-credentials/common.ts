import { KafkaTSApiError } from '../../utils/error';

export type DescribeUserScramCredentialsRequest = {
    users:
        | {
              name: string;
          }[]
        | null;
};

export type DescribeUserScramCredentialsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    results: {
        user: string;
        errorCode: number;
        errorMessage: string | null;
        credentialInfos: {
            mechanism: number;
            iterations: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeUserScramCredentialsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    result.results.forEach((resultItem) => {
        if (resultItem.errorCode) throw new KafkaTSApiError(resultItem.errorCode, resultItem.errorMessage, result);
    });
    return result;
};
