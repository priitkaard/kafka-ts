import { KafkaTSApiError } from '../../utils/error';

export type DescribeAclsRequest = {
    resourceTypeFilter: number;
    resourceNameFilter: string | null;
    patternTypeFilter: number;
    principalFilter: string | null;
    hostFilter: string | null;
    operation: number;
    permissionType: number;
};

export type DescribeAclsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    resources: {
        resourceType: number;
        resourceName: string;
        patternType: number;
        acls: {
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

export const throwIfError = <T extends DescribeAclsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
