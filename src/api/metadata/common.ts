import { KafkaTSApiError } from '../../utils/error';

export type MetadataRequest = {
    topics?: { id: string | null; name: string }[] | null;
    allowTopicAutoCreation?: boolean;
    includeClusterAuthorizedOperations?: boolean;
    includeTopicAuthorizedOperations?: boolean;
};

export type MetadataResponse = {
    throttleTimeMs: number;
    brokers: {
        nodeId: number;
        host: string;
        port: number;
        rack: string | null;
    }[];
    clusterId: string | null;
    controllerId: number;
    topics: {
        errorCode: number;
        name: string;
        topicId: string;
        isInternal: boolean;
        partitions: {
            errorCode: number;
            partitionIndex: number;
            leaderId: number;
            leaderEpoch: number;
            replicaNodes: number[];
            isrNodes: number[];
            offlineReplicas: number[];
            tags: Record<number, Buffer>;
        }[];
        topicAuthorizedOperations: number;
        tags: Record<number, Buffer>;
    }[];
    clusterAuthorizedOperations: number;
    errorCode: number;
    tags: Record<number, Buffer>;
};
export type Metadata = MetadataResponse;

export const AUTHORIZED_OPERATIONS_OMITTED = -2147483648;

export const throwIfError = (result: MetadataResponse) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    result.topics.forEach((topic) => {
        if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, null, result);
        topic.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};
