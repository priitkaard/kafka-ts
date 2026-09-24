import { KafkaTSApiError } from '../../utils/error';

export type CreateTopicsRequest = {
    topics: {
        name: string;
        numPartitions?: number;
        replicationFactor?: number;
        assignments?: {
            partitionIndex: number;
            brokerIds: number[];
        }[];
        configs?: {
            name: string;
            value: string | null;
        }[];
    }[];
    timeoutMs?: number;
    validateOnly?: boolean;
};

export type CreateTopicsResponse = {
    throttleTimeMs: number;
    topics: {
        name: string;
        _topicId: string;
        errorCode: number;
        errorMessage: string | null;
        _numPartitions: number;
        _replicationFactor: number;
        configs: {
            name: string;
            value: string | null;
            readOnly: boolean;
            configSource: number;
            isSensitive: boolean;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: CreateTopicsResponse) => {
    result.topics.forEach((topic) => {
        if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, topic.errorMessage, result);
    });
    return result;
};
