import { KafkaTSApiError } from '../../utils/error';

export type IncrementalAlterConfigsRequest = {
    resources: {
        resourceType: number;
        resourceName: string;
        configs: {
            name: string;
            configOperation: number;
            value: string | null;
        }[];
    }[];
    validateOnly: boolean;
};

export type IncrementalAlterConfigsResponse = {
    throttleTimeMs: number;
    responses: {
        errorCode: number;
        errorMessage: string | null;
        resourceType: number;
        resourceName: string;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends IncrementalAlterConfigsResponse>(result: T) => {
    result.responses.forEach((response) => {
        if (response.errorCode) throw new KafkaTSApiError(response.errorCode, response.errorMessage, result);
    });
    return result;
};
