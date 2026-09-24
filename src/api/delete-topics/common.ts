import { KafkaTSApiError } from '../../utils/error';

export type DeleteTopicsRequest = {
    topics: {
        name: string;
        topicId: string | null;
    }[];
    timeoutMs?: number;
};

export type DeleteTopicsResponse = {
    throttleTimeMs: number;
    responses: {
        name: string | null;
        _topicId: string;
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: DeleteTopicsResponse) => {
    result.responses.forEach((response) => {
        if (response.errorCode) throw new KafkaTSApiError(response.errorCode, response.errorMessage, result);
    });
    return result;
};
