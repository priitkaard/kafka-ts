import { KafkaTSApiError } from '../../utils/error';

export type CreateAclsRequest = {
    creations: {
        resourceType: number;
        resourceName: string;
        resourcePatternType: number;
        principal: string;
        host: string;
        operation: number;
        permissionType: number;
    }[];
};

export type CreateAclsResponse = {
    throttleTimeMs: number;
    results: {
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends CreateAclsResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        if (resultItem.errorCode) throw new KafkaTSApiError(resultItem.errorCode, resultItem.errorMessage, result);
    });
    return result;
};
