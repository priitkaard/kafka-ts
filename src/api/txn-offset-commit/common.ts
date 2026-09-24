import { KafkaTSApiError } from '../../utils/error';

export type TxnOffsetCommitRequest = {
    transactionalId: string;
    groupId: string;
    producerId: bigint;
    producerEpoch: number;
    generationId?: number;
    memberId?: string;
    groupInstanceId?: string | null;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            committedOffset: bigint;
            committedLeaderEpoch?: number;
            committedMetadata: string | null;
        }[];
    }[];
};

export type TxnOffsetCommitResponse = {
    throttleTimeMs: number;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends TxnOffsetCommitResponse>(result: T) => {
    result.topics.forEach((topic) => {
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
