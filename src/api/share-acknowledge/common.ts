import { KafkaTSApiError } from '../../utils/error';

export type ShareAcknowledgeRequest = {
    groupId: string | null;
    memberId: string | null;
    shareSessionEpoch: number;
    isRenewAck?: boolean;
    topics: {
        topicId: string | null;
        partitions: {
            partitionIndex: number;
            acknowledgementBatches: {
                firstOffset: bigint;
                lastOffset: bigint;
                acknowledgeTypes: number[];
            }[];
        }[];
    }[];
};

export type ShareAcknowledgeResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    acquisitionLockTimeoutMs: number;
    responses: {
        topicId: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            errorMessage: string | null;
            currentLeader: {
                leaderId: number;
                leaderEpoch: number;
                tags: Record<number, Buffer>;
            };
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    nodeEndpoints: {
        nodeId: number;
        host: string;
        port: number;
        rack: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends ShareAcknowledgeResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    result.responses.forEach((response) => {
        response.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
        });
    });
    return result;
};
