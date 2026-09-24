import { KafkaTSApiError } from '../../utils/error';

export type WriteTxnMarkersRequest = {
    markers: {
        producerId: bigint;
        producerEpoch: number;
        transactionResult: boolean;
        topics: {
            name: string;
            partitionIndexes: number[];
        }[];
        coordinatorEpoch: number;
        transactionVersion?: number;
    }[];
};

export type WriteTxnMarkersResponse = {
    markers: {
        producerId: bigint;
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
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends WriteTxnMarkersResponse>(result: T) => {
    result.markers.forEach((marker) => {
        marker.topics.forEach((topic) => {
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
    });
    return result;
};
