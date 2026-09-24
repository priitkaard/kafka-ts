import { KafkaTSApiError } from '../../utils/error';

export type CreatePartitionsRequest = {
    topics: {
        name: string;
        count: number;
        assignments:
            | {
                  brokerIds: number[];
              }[]
            | null;
    }[];
    timeoutMs: number;
    validateOnly: boolean;
};

export type CreatePartitionsResponse = {
    throttleTimeMs: number;
    results: {
        name: string;
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends CreatePartitionsResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        if (resultItem.errorCode) throw new KafkaTSApiError(resultItem.errorCode, resultItem.errorMessage, result);
    });
    return result;
};
