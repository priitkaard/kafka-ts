import { KafkaTSApiError } from '../../utils/error';

export type StreamsGroupHeartbeatRequest = {
    groupId: string;
    memberId: string;
    memberEpoch: number;
    endpointInformationEpoch: number;
    instanceId: string | null;
    rackId: string | null;
    rebalanceTimeoutMs: number;
    topology: {
        epoch: number;
        subtopologies: {
            subtopologyId: string;
            sourceTopics: string[];
            sourceTopicRegex: string[];
            stateChangelogTopics: {
                name: string;
                partitions: number;
                replicationFactor: number;
                topicConfigs: {
                    key: string;
                    value: string;
                }[];
            }[];
            repartitionSinkTopics: string[];
            repartitionSourceTopics: {
                name: string;
                partitions: number;
                replicationFactor: number;
                topicConfigs: {
                    key: string;
                    value: string;
                }[];
            }[];
            copartitionGroups: {
                sourceTopics: number[];
                sourceTopicRegex: number[];
                repartitionSourceTopics: number[];
            }[];
        }[];
    } | null;
    activeTasks:
        | {
              subtopologyId: string;
              partitions: number[];
          }[]
        | null;
    standbyTasks:
        | {
              subtopologyId: string;
              partitions: number[];
          }[]
        | null;
    warmupTasks:
        | {
              subtopologyId: string;
              partitions: number[];
          }[]
        | null;
    processId: string | null;
    userEndpoint: {
        host: string;
        port: number;
    } | null;
    clientTags:
        | {
              key: string;
              value: string;
          }[]
        | null;
    taskOffsets:
        | {
              subtopologyId: string;
              partition: number;
              offset: bigint;
          }[]
        | null;
    taskEndOffsets:
        | {
              subtopologyId: string;
              partition: number;
              offset: bigint;
          }[]
        | null;
    shutdownApplication: boolean;
};

export type StreamsGroupHeartbeatResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    memberId: string;
    memberEpoch: number;
    heartbeatIntervalMs: number;
    acceptableRecoveryLag: number;
    taskOffsetIntervalMs: number;
    status: {
        statusCode: number;
        statusDetail: string;
        tags: Record<number, Buffer>;
    }[];
    activeTasks: {
        subtopologyId: string;
        partitions: number[];
        tags: Record<number, Buffer>;
    }[];
    standbyTasks: {
        subtopologyId: string;
        partitions: number[];
        tags: Record<number, Buffer>;
    }[];
    warmupTasks: {
        subtopologyId: string;
        partitions: number[];
        tags: Record<number, Buffer>;
    }[];
    endpointInformationEpoch: number;
    partitionsByUserEndpoint: {
        userEndpoint: {
            host: string;
            port: number;
            tags: Record<number, Buffer>;
        };
        activePartitions: {
            topic: string;
            partitions: number[];
            tags: Record<number, Buffer>;
        }[];
        standbyPartitions: {
            topic: string;
            partitions: number[];
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends StreamsGroupHeartbeatResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
