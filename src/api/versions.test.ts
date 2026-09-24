import { describe, expect } from 'vitest';
import { API, API_ERROR } from '.';
import { Connection } from '../connection';
import {
    connectionOptions,
    createPartitionData,
    createVersionTestContext,
    eventually,
    randomName,
    ssl,
} from './versions.test-utils';
import { KEY_TYPE } from './find-coordinator';

describe.sequential('API versions', () => {
    const context = createVersionTestContext('versions');
    const {
        topicName,
        forEachVersion,
        createTopic,
        findCoordinator,
        joinGroup,
        joinAndSyncGroup,
        leaveGroup,
        commitOffset,
    } = context;

    describe('ApiVersions', () => {
        forEachVersion(API.API_VERSIONS, async (api) => {
            const { versions } = await context.cluster.sendRequest(api, {});
            expect(versions).toContainEqual(expect.objectContaining({ apiKey: API.API_VERSIONS.apiKey }));
        });
    });

    describe('SaslHandshake', () => {
        forEachVersion(API.SASL_HANDSHAKE, async (api) => {
            const connection = new Connection({
                clientId: 'kafka-ts',
                connection: connectionOptions,
                ssl,
                requestTimeout: 10_000,
                connectTimeout: 10_000,
            });
            await connection.connect();
            try {
                const { mechanisms } = await connection.sendRequest(api, { mechanism: 'PLAIN' });
                expect(mechanisms).toContain('PLAIN');
            } finally {
                await connection.disconnect();
            }
        });
    });

    describe('SaslAuthenticate', () => {
        forEachVersion(API.SASL_AUTHENTICATE, async (api) => {
            const connection = new Connection({
                clientId: 'kafka-ts',
                connection: connectionOptions,
                ssl,
                requestTimeout: 10_000,
                connectTimeout: 10_000,
            });
            await connection.connect();
            try {
                await connection.sendRequest(API.SASL_HANDSHAKE, { mechanism: 'PLAIN' });
                const result = await connection.sendRequest(api, { authBytes: Buffer.from('\u0000admin\u0000admin') });
                expect(result.errorCode).toBe(0);
            } finally {
                await connection.disconnect();
            }
        });
    });

    describe('Metadata', () => {
        forEachVersion(API.METADATA, async (api) => {
            const { topics, brokers } = await context.cluster.sendRequest(api, {
                topics: [{ id: null, name: topicName }],
            });
            expect(brokers.length).toBeGreaterThan(0);
            expect(topics).toEqual([
                expect.objectContaining({
                    name: topicName,
                    topicId: api.apiVersion >= 10 ? context.topicId : '',
                    partitions: [expect.objectContaining({ partitionIndex: 0, leaderId: context.leaderId })],
                }),
            ]);
        });
    });

    describe('CreateTopics', () => {
        forEachVersion(API.CREATE_TOPICS, async (api) => {
            const name = randomName(`create-v${api.apiVersion}`);
            const { topics } = await context.cluster.sendRequest(api, {
                topics: [{ name, numPartitions: 1, replicationFactor: 1 }],
                validateOnly: true,
            });
            expect(topics).toEqual([expect.objectContaining({ name, errorCode: 0 })]);
        });
    });

    describe('DeleteTopics', () => {
        forEachVersion(API.DELETE_TOPICS, async (api) => {
            const name = randomName(`delete-v${api.apiVersion}`);
            await createTopic(name);
            const { responses } = await context.cluster.sendRequest(api, { topics: [{ name, topicId: null }] });
            expect(responses).toEqual([expect.objectContaining({ name, errorCode: 0 })]);
        });
    });

    describe('Produce', () => {
        forEachVersion(API.PRODUCE, async (api) => {
            const { responses } = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                transactionalId: null,
                acks: -1,
                timeoutMs: 10_000,
                topicData: [
                    {
                        name: topicName,
                        topicId: context.topicId,
                        partitionData: [createPartitionData(`v${api.apiVersion}`)],
                    },
                ],
            });
            expect(responses).toEqual([
                expect.objectContaining(api.apiVersion >= 13 ? { topicId: context.topicId } : { name: topicName }),
            ]);
            expect(responses[0].partitionResponses[0].baseOffset).toBeGreaterThan(0n);
        });
    });

    describe('ListOffsets', () => {
        forEachVersion(API.LIST_OFFSETS, async (api) => {
            const { topics } = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                replicaId: -1,
                isolationLevel: 0,
                topics: [
                    { name: topicName, partitions: [{ partitionIndex: 0, currentLeaderEpoch: -1, timestamp: -1n }] },
                ],
            });
            expect(topics[0].name).toBe(topicName);
            expect(topics[0].partitions[0].offset).toBeGreaterThan(0n);
        });
    });

    describe('Fetch', () => {
        forEachVersion(API.FETCH, async (api) => {
            const { responses } = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                maxWaitMs: 100,
                minBytes: 1,
                maxBytes: 1_048_576,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [
                    {
                        topicId: context.topicId,
                        topicName,
                        partitions: [
                            {
                                partition: 0,
                                currentLeaderEpoch: -1,
                                fetchOffset: 0n,
                                lastFetchedEpoch: -1,
                                logStartOffset: -1n,
                                partitionMaxBytes: 1_048_576,
                            },
                        ],
                    },
                ],
                forgottenTopicsData: [],
                rackId: '',
            });
            expect(responses).toEqual([
                expect.objectContaining(api.apiVersion >= 13 ? { topicId: context.topicId } : { topicName }),
            ]);
            const [firstBatch] = responses[0].partitions[0].records;
            expect(firstBatch.records[0]).toMatchObject({
                key: 'key',
                value: 'initial',
                headers: [{ key: 'header-key', value: 'header-value' }],
            });
        });
    });

    describe('InitProducerId', () => {
        forEachVersion(API.INIT_PRODUCER_ID, async (api) => {
            const result = await context.cluster.sendRequest(api, {
                transactionalId: null,
                transactionTimeoutMs: 60_000,
                producerId: -1n,
                producerEpoch: -1,
            });
            expect(result.producerId).toBeGreaterThanOrEqual(0n);
        });
    });

    describe('FindCoordinator', () => {
        forEachVersion(API.FIND_COORDINATOR, async (api) => {
            const { coordinators } = await eventually(() =>
                context.cluster.sendRequest(api, { keyType: KEY_TYPE.GROUP, keys: [randomName('group')] }),
            );
            expect(coordinators).toEqual([expect.objectContaining({ errorCode: 0, port: expect.any(Number) })]);
        });
    });

    describe('JoinGroup', () => {
        forEachVersion(API.JOIN_GROUP, async (api) => {
            const groupId = randomName(`join-v${api.apiVersion}`);
            const { response } = await joinGroup(groupId, api);
            expect(response.leader).toBe(response.memberId);
            expect(response.members.map(({ memberId }) => memberId)).toEqual([response.memberId]);
            await leaveGroup(groupId, response.memberId);
        });
    });

    describe('SyncGroup', () => {
        forEachVersion(API.SYNC_GROUP, async (api) => {
            const groupId = randomName(`sync-v${api.apiVersion}`);
            const { sendRequest, response } = await joinGroup(groupId);
            const { memberId, generationId } = response;
            const { assignment } = await sendRequest(api, {
                groupId,
                generationId,
                memberId,
                groupInstanceId: null,
                protocolType: 'consumer',
                protocolName: 'RoundRobinAssigner',
                assignments: [{ memberId, assignment: { [topicName]: [0] } }],
            });
            expect(assignment).toEqual({ [topicName]: [0] });
            await leaveGroup(groupId, memberId);
        });
    });

    describe('Heartbeat', () => {
        forEachVersion(API.HEARTBEAT, async (api) => {
            const groupId = randomName(`heartbeat-v${api.apiVersion}`);
            const { sendRequest, memberId, generationId } = await joinAndSyncGroup(groupId);
            const { errorCode } = await sendRequest(api, { groupId, generationId, memberId, groupInstanceId: null });
            expect(errorCode).toBe(0);
            await leaveGroup(groupId, memberId);
        });
    });

    describe('LeaveGroup', () => {
        forEachVersion(API.LEAVE_GROUP, async (api) => {
            const groupId = randomName(`leave-v${api.apiVersion}`);
            const { sendRequest, memberId, generationId } = await joinAndSyncGroup(groupId);
            const { errorCode } = await sendRequest(api, {
                groupId,
                members: [{ memberId, groupInstanceId: null, reason: 'test' }],
            });
            expect(errorCode).toBe(0);
            await expect(
                sendRequest(API.HEARTBEAT, { groupId, generationId, memberId, groupInstanceId: null }),
            ).rejects.toMatchObject({ errorCode: API_ERROR.UNKNOWN_MEMBER_ID });
        });
    });

    describe('OffsetCommit', () => {
        forEachVersion(API.OFFSET_COMMIT, async (api) => {
            const groupId = randomName(`commit-v${api.apiVersion}`);
            const sendRequest = await findCoordinator(groupId);
            const { topics } = await sendRequest(api, {
                groupId,
                generationIdOrMemberEpoch: -1,
                memberId: '',
                groupInstanceId: null,
                topics: [
                    {
                        name: topicName,
                        topicId: context.topicId,
                        partitions: [
                            {
                                partitionIndex: 0,
                                committedOffset: BigInt(api.apiVersion),
                                committedLeaderEpoch: -1,
                                committedMetadata: null,
                            },
                        ],
                    },
                ],
            });
            expect(topics).toEqual([
                expect.objectContaining(api.apiVersion >= 10 ? { topicId: context.topicId } : { name: topicName }),
            ]);

            const { groups } = await sendRequest(API.OFFSET_FETCH, {
                groups: [{ groupId, topics: [{ name: topicName, topicId: context.topicId, partitionIndexes: [0] }] }],
                requireStable: false,
            });
            expect(groups[0].topics[0].partitions[0].committedOffset).toBe(BigInt(api.apiVersion));
        });
    });

    describe('OffsetFetch', () => {
        forEachVersion(API.OFFSET_FETCH, async (api) => {
            const groupId = randomName(`fetch-offsets-v${api.apiVersion}`);
            const sendRequest = await commitOffset(groupId);

            const { groups } = await sendRequest(api, {
                groups: [{ groupId, topics: [{ name: topicName, topicId: context.topicId, partitionIndexes: [0] }] }],
                requireStable: true,
            });
            expect(groups[0].topics).toEqual([
                expect.objectContaining(api.apiVersion >= 10 ? { topicId: context.topicId } : { name: topicName }),
            ]);
            expect(groups[0].topics[0].partitions[0]).toMatchObject({
                partitionIndex: 0,
                committedOffset: 1n,
                committedMetadata: 'meta',
            });
        });
    });
});
