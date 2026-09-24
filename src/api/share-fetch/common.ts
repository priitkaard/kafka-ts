import { KafkaTSApiError } from '../../utils/error';
import { decodeRecordBatch } from '../fetch';

type RecordBatch = ReturnType<typeof decodeRecordBatch>[number];

export type ShareFetchRequest = {
    groupId: string | null;
    memberId: string | null;
    shareSessionEpoch: number;
    maxWaitMs: number;
    minBytes: number;
    maxBytes: number;
    maxRecords: number;
    batchSize: number;
    shareAcquireMode?: number;
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
    forgottenTopicsData: {
        topicId: string | null;
        partitions: number[];
    }[];
};

export type ShareFetchResponse = {
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
            acknowledgeErrorCode: number;
            acknowledgeErrorMessage: string | null;
            currentLeader: {
                leaderId: number;
                leaderEpoch: number;
                tags: Record<number, Buffer>;
            };
            records: RecordBatch[];
            acquiredRecords: {
                firstOffset: bigint;
                lastOffset: bigint;
                deliveryCount: number;
                tags: Record<number, Buffer>;
            }[];
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

export const throwIfError = <T extends ShareFetchResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    result.responses.forEach((response) => {
        response.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, partition.errorMessage, result);
            if (partition.acknowledgeErrorCode)
                throw new KafkaTSApiError(partition.acknowledgeErrorCode, partition.acknowledgeErrorMessage, result);
        });
    });
    return result;
};
