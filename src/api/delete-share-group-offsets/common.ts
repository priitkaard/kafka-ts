import { KafkaTSApiError } from '../../utils/error';

export type DeleteShareGroupOffsetsRequest = {
    groupId: string;
    topics: {
        topicName: string;
    }[];
};

export type DeleteShareGroupOffsetsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    responses: {
        topicName: string;
        topicId: string;
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DeleteShareGroupOffsetsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    result.responses.forEach((response) => {
        if (response.errorCode) throw new KafkaTSApiError(response.errorCode, response.errorMessage, result);
    });
    return result;
};
