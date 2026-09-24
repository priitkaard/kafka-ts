import { KafkaTSApiError } from '../../utils/error';

export type DeleteGroupsRequest = {
    groupsNames: string[];
};

export type DeleteGroupsResponse = {
    throttleTimeMs: number;
    results: {
        groupId: string;
        errorCode: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DeleteGroupsResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        if (resultItem.errorCode) throw new KafkaTSApiError(resultItem.errorCode, null, result);
    });
    return result;
};
