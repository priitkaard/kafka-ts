import { randomBytes } from 'crypto';
import { readFileSync } from 'fs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { API } from './api';
import { KEY_TYPE } from './api/find-coordinator';
import { saslPlain } from './auth';
import { createKafkaClient } from './client';
import { Cluster } from './cluster';
import { KafkaTSApiError } from './utils/error';

export const kafka = createKafkaClient({
    clientId: 'kafka-ts',
    bootstrapServers: [{ host: 'localhost', port: 9092 }],
    sasl: saslPlain({ username: 'admin', password: 'admin' }),
    ssl: { ca: readFileSync('./certs/ca.crt').toString() },
});

describe.sequential('Request handler', () => {
    const groupId = randomBytes(16).toString('hex');

    let cluster: Cluster;

    beforeAll(async () => {
        cluster = await kafka.createCluster().connect();

        const metadataResult = await cluster.sendRequest(API.METADATA, {
            topics: null,
            allowTopicAutoCreation: false,
            includeTopicAuthorizedOperations: false,
        });
        if (metadataResult.topics.some((topic) => topic.name === 'kafka-ts-test-topic')) {
            await cluster.sendRequest(API.DELETE_TOPICS, {
                topics: [{ name: 'kafka-ts-test-topic', topicId: null }],
                timeoutMs: 10000,
            });
        }
    });

    afterAll(async () => {
        await cluster.disconnect();
    });

    it('should request api versions', async () => {
        const result = await cluster.sendRequest(API.API_VERSIONS, {});
        expect(result).toMatchSnapshot();
    });

    let topicId: string = 'd6718d178e1b47c886441ad2d19faea5';

    it('should create topics', async () => {
        const result = await cluster.sendRequest(API.CREATE_TOPICS, {
            topics: [
                {
                    name: 'kafka-ts-test-topic',
                    numPartitions: 1,
                    replicationFactor: 1,
                    assignments: [],
                    configs: [],
                },
            ],
            timeoutMs: 10000,
            validateOnly: false,
        });
        topicId = result.topics[0].topicId;
        result.topics.forEach((topic) => {
            topic.topicId = 'Any<UUID>';
        });
        expect(result).toMatchSnapshot();

        await new Promise((resolve) => setTimeout(resolve, 1000));
    });

    it('should request metadata for all topics', async () => {
        const result = await cluster.sendRequest(API.METADATA, {
            topics: null,
            allowTopicAutoCreation: false,
            includeTopicAuthorizedOperations: false,
        });
        result.controllerId = 0;
        result.topics = result.topics.filter((topic) => topic.name !== '__consumer_offsets');
        result.topics.forEach((topic) => {
            topic.topicId = 'Any<UUID>';
            topic.partitions.forEach((partition) => {
                partition.leaderId = 0;
                partition.isrNodes = [0];
                partition.replicaNodes = [0];
            });
        });
        expect(result).toMatchSnapshot();
    });

    let leaderId = 0;

    it('should request metadata for a topic', async () => {
        const result = await cluster.sendRequest(API.METADATA, {
            topics: [{ id: topicId, name: 'kafka-ts-test-topic' }],
            allowTopicAutoCreation: false,
            includeTopicAuthorizedOperations: false,
        });
        leaderId = result.topics[0].partitions[0].leaderId;
        result.controllerId = 0;
        result.topics.forEach((topic) => {
            topic.topicId = 'Any<UUID>';
            topic.partitions.forEach((partition) => {
                partition.leaderId = 0;
                partition.isrNodes = [0];
                partition.replicaNodes = [0];
            });
        });
        expect(result).toMatchSnapshot();
    });

    let producerId = 9n;

    it('should init producer id', async () => {
        const result = await cluster.sendRequest(API.INIT_PRODUCER_ID, {
            transactionalId: null,
            transactionTimeoutMs: 0,
            producerId,
            producerEpoch: 0,
        });
        result.producerId = 0n;
        expect(result).toMatchSnapshot();
    });

    it('should produce messages', async () => {
        const now = Date.now();
        const result = await cluster.sendRequestToNode(leaderId)(API.PRODUCE, {
            transactionalId: null,
            timeoutMs: 10000,
            acks: 1,
            topicData: [
                {
                    name: 'kafka-ts-test-topic',
                    partitionData: [
                        {
                            index: 0,
                            baseOffset: 0n,
                            partitionLeaderEpoch: 0,
                            attributes: 0,
                            baseSequence: 0,
                            baseTimestamp: BigInt(now),
                            lastOffsetDelta: 0,
                            maxTimestamp: BigInt(now),
                            producerEpoch: 0,
                            producerId,
                            records: [
                                {
                                    attributes: 0,
                                    offsetDelta: 0,
                                    timestampDelta: 0n,
                                    key: 'key',
                                    value: 'value',
                                    headers: [
                                        {
                                            key: 'header-key',
                                            value: 'header-value',
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        });
        expect(result).toMatchSnapshot();
    });

    it('should fetch messages', async () => {
        const result = await cluster.sendRequestToNode(leaderId)(API.FETCH, {
            maxWaitMs: 100,
            minBytes: 1,
            maxBytes: 10485760,
            isolationLevel: 1,
            sessionId: 0,
            sessionEpoch: -1,
            topics: [
                {
                    topicId,
                    partitions: [
                        {
                            partition: 0,
                            currentLeaderEpoch: -1,
                            fetchOffset: 0n,
                            lastFetchedEpoch: 0,
                            logStartOffset: -1n,
                            partitionMaxBytes: 10485760,
                        },
                    ],
                },
            ],
            forgottenTopicsData: [],
            rackId: '',
        });
        result.responses.forEach((response) => {
            response.topicId = 'Any<UUID>';
            response.partitions.forEach((partition) => {
                partition.records.forEach((record) => {
                    expect(record.baseTimestamp).toBeGreaterThan(1721926744730n);
                    expect(record.maxTimestamp).toBeGreaterThan(1721926744730n);
                    expect(record.crc).toBeGreaterThan(0);

                    record.baseTimestamp = 0n;
                    record.maxTimestamp = 0n;
                    record.crc = 0;
                });
            });
        });
        expect(result).toMatchSnapshot();
    });

    let coordinatorId = -1;

    it('should find coordinator', async () => {
        const result = await cluster.sendRequest(API.FIND_COORDINATOR, { keyType: KEY_TYPE.GROUP, keys: [groupId] });
        result.coordinators.forEach((coordinator) => {
            coordinator.key = 'Any<String>';
        });
        coordinatorId = result.coordinators[0].nodeId;
        result.coordinators.forEach((coordinator) => {
            coordinator.nodeId = 1;
            coordinator.port = 9093;
        });
        expect(result).toMatchSnapshot();
    });

    let memberId = '';

    it('should fail join group request with new memberId', async () => {
        try {
            const result = await cluster.sendRequestToNode(coordinatorId)(API.JOIN_GROUP, {
                groupId,
                sessionTimeoutMs: 30000,
                rebalanceTimeoutMs: 60000,
                memberId,
                groupInstanceId: null,
                protocolType: 'consumer',
                protocols: [
                    {
                        name: 'RoundRobinAssigner',
                        metadata: { version: 0, topics: ['kafka-ts-test-topic'] },
                    },
                ],
                reason: null,
            });
            expect(false, 'Should throw an error').toBe(true);
        } catch (error) {
            const { response } = error as KafkaTSApiError;
            memberId = response.memberId;
            response.memberId = 'Any<UUID>';
            expect(response).toMatchSnapshot();
        }
    });

    it('should join group', async () => {
        const result = await cluster.sendRequestToNode(coordinatorId)(API.JOIN_GROUP, {
            groupId,
            sessionTimeoutMs: 30000,
            rebalanceTimeoutMs: 60000,
            memberId,
            groupInstanceId: null,
            protocolType: 'consumer',
            protocols: [
                {
                    name: 'RoundRobinAssigner',
                    metadata: { version: 0, topics: ['kafka-ts-test-topic'] },
                },
            ],
            reason: null,
        });
        result.memberId = 'Any<UUID>';
        result.leader = 'Any<UUID>';
        result.members.forEach((member) => {
            member.memberId = 'Any<UUID>';
        });
        expect(result).toMatchSnapshot();
    });

    it('should sync group', async () => {
        const result = await cluster.sendRequestToNode(coordinatorId)(API.SYNC_GROUP, {
            groupId,
            generationId: 1,
            memberId,
            groupInstanceId: null,
            protocolType: 'consumer',
            protocolName: 'RoundRobinAssigner',
            assignments: [
                {
                    memberId,
                    assignment: { 'kafka-test-topic': [0] },
                },
            ],
        });
        expect(result).toMatchSnapshot();
    });

    it('should commit offsets', async () => {
        const result = await cluster.sendRequestToNode(coordinatorId)(API.OFFSET_COMMIT, {
            groupId,
            generationIdOrMemberEpoch: 1,
            memberId,
            groupInstanceId: null,
            topics: [
                {
                    name: 'kafka-ts-test-topic',
                    partitions: [
                        { partitionIndex: 0, committedOffset: 1n, committedLeaderEpoch: 0, committedMetadata: null },
                    ],
                },
            ],
        });
        expect(result).toMatchSnapshot();
    });

    it('should fetch offsets', async () => {
        const result = await cluster.sendRequestToNode(coordinatorId)(API.OFFSET_FETCH, {
            groups: [
                {
                    groupId,
                    memberId,
                    memberEpoch: 0,
                    topics: [
                        {
                            name: 'kafka-ts-test-topic',
                            partitionIndexes: [0],
                        },
                    ],
                },
            ],
            requireStable: false,
        });
        result.groups.forEach((group) => {
            group.groupId = 'Any<String>';
        });
        expect(result).toMatchSnapshot();
    });

    it('should heartbeat', async () => {
        const result = await cluster.sendRequestToNode(coordinatorId)(API.HEARTBEAT, {
            groupId,
            generationId: 1,
            memberId,
            groupInstanceId: null,
        });
        expect(result).toMatchSnapshot();
    });

    it('should leave group', async () => {
        const result = await cluster.sendRequestToNode(coordinatorId)(API.LEAVE_GROUP, {
            groupId,
            members: [{ memberId, groupInstanceId: null, reason: null }],
        });
        result.members.forEach((member) => {
            member.memberId = 'Any<UUID>';
        });
        expect(result).toMatchSnapshot();
    });

    it('should delete topics', async () => {
        const result = await cluster.sendRequest(API.DELETE_TOPICS, {
            topics: [{ name: 'kafka-ts-test-topic', topicId: null }],
            timeoutMs: 10000,
        });
        result.responses.forEach((response) => {
            response.topicId = 'Any<UUID>';
        });
        expect(result).toMatchSnapshot();
    });
});
