import { KafkaTSApiError } from '../../utils/error';

export type ListConfigResourcesRequest = {
    resourceTypes?: number[];
};

export type ListConfigResourcesResponse = {
    throttleTimeMs: number;
    errorCode: number;
    configResources: {
        resourceName: string;
        resourceType: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ListConfigResourcesResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
