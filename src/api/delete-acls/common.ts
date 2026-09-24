import { KafkaTSApiError } from '../../utils/error';

export type DeleteAclsRequest = {
    filters: {
        resourceTypeFilter: number;
        resourceNameFilter: string | null;
        patternTypeFilter: number;
        principalFilter: string | null;
        hostFilter: string | null;
        operation: number;
        permissionType: number;
    }[];
};

export type DeleteAclsResponse = {
    throttleTimeMs: number;
    filterResults: {
        errorCode: number;
        errorMessage: string | null;
        matchingAcls: {
            errorCode: number;
            errorMessage: string | null;
            resourceType: number;
            resourceName: string;
            patternType: number;
            principal: string;
            host: string;
            operation: number;
            permissionType: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DeleteAclsResponse>(result: T) => {
    result.filterResults.forEach((filterResult) => {
        if (filterResult.errorCode)
            throw new KafkaTSApiError(filterResult.errorCode, filterResult.errorMessage, result);
        filterResult.matchingAcls.forEach((matchingAcl) => {
            if (matchingAcl.errorCode)
                throw new KafkaTSApiError(matchingAcl.errorCode, matchingAcl.errorMessage, result);
        });
    });
    return result;
};
