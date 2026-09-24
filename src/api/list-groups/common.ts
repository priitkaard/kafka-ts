import { KafkaTSApiError } from '../../utils/error';

export type ListGroupsRequest = {
    statesFilter?: string[];
    typesFilter?: string[];
};

export type ListGroupsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    groups: {
        groupId: string;
        protocolType: string;
        groupState: string;
        groupType: string;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ListGroupsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
