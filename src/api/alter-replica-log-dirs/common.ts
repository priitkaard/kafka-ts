import { KafkaTSApiError } from '../../utils/error';

export type AlterReplicaLogDirsRequest = {
    dirs: {
        path: string;
        topics: {
            name: string;
            partitions: number[];
        }[];
    }[];
};

export type AlterReplicaLogDirsResponse = {
    throttleTimeMs: number;
    results: {
        topicName: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends AlterReplicaLogDirsResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        resultItem.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
