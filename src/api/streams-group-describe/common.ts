import { KafkaTSApiError } from '../../utils/error';

export type StreamsGroupDescribeRequest = {
    groupIds: string[];
    includeAuthorizedOperations: boolean;
};

export type StreamsGroupDescribeResponse = {
    throttleTimeMs: number;
    groups: {
        errorCode: number;
        errorMessage: string | null;
        groupId: string;
        groupState: string;
        groupEpoch: number;
        assignmentEpoch: number;
        topology: {
            epoch: number;
            subtopologies: {
                subtopologyId: string;
                sourceTopics: string[];
                repartitionSinkTopics: string[];
                stateChangelogTopics: {
                    name: string;
                    partitions: number;
                    replicationFactor: number;
                    topicConfigs: {
                        key: string;
                        value: string;
                        tags: Record<number, Buffer>;
                    }[];
                    tags: Record<number, Buffer>;
                }[];
                repartitionSourceTopics: {
                    name: string;
                    partitions: number;
                    replicationFactor: number;
                    topicConfigs: {
                        key: string;
                        value: string;
                        tags: Record<number, Buffer>;
                    }[];
                    tags: Record<number, Buffer>;
                }[];
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        } | null;
        members: {
            memberId: string;
            memberEpoch: number;
            instanceId: string | null;
            rackId: string | null;
            clientId: string;
            clientHost: string;
            topologyEpoch: number;
            processId: string;
            userEndpoint: {
                host: string;
                port: number;
                tags: Record<number, Buffer>;
            } | null;
            clientTags: {
                key: string;
                value: string;
                tags: Record<number, Buffer>;
            }[];
            taskOffsets: {
                subtopologyId: string;
                partition: number;
                offset: bigint;
                tags: Record<number, Buffer>;
            }[];
            taskEndOffsets: {
                subtopologyId: string;
                partition: number;
                offset: bigint;
                tags: Record<number, Buffer>;
            }[];
            assignment: {
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
                tags: Record<number, Buffer>;
            };
            targetAssignment: {
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
                tags: Record<number, Buffer>;
            };
            isClassic: boolean;
            tags: Record<number, Buffer>;
        }[];
        authorizedOperations: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends StreamsGroupDescribeResponse>(result: T) => {
    result.groups.forEach((group) => {
        if (group.errorCode) throw new KafkaTSApiError(group.errorCode, group.errorMessage, result);
    });
    return result;
};
