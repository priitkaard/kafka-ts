import { randomBytes } from 'crypto';
import { readFileSync } from 'fs';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { API, API_ERROR } from '.';
import { saslPlain } from '../auth';
import { createKafkaClient } from '../client';
import { Cluster } from '../cluster';
import { Connection } from '../connection';
import { Api } from '../utils/api';
import { delay } from '../utils/delay';
import { KafkaTSApiError } from '../utils/error';
import { KEY_TYPE } from './find-coordinator';

const connectionOptions = { host: 'localhost', port: 39092 };
const ssl = { ca: readFileSync('./certs/ca.crt').toString() };

const kafka = createKafkaClient({
    clientId: 'kafka-ts',
    bootstrapServers: [connectionOptions],
    sasl: saslPlain({ username: 'admin', password: 'admin' }),
    ssl,
});

const getVersions = <Request, Response>(api: Api<Request, Response>) => {
    const versions: Api<Request, Response>[] = [];
    for (let version: Api<Request, Response> | undefined = api; version; version = version.fallback) {
        versions.unshift(version);
    }
    return versions;
};

const randomName = (prefix: string) => `kafka-ts-${prefix}-${randomBytes(6).toString('hex')}`;

const eventually = async <T>(callback: () => Promise<T>, attempts = 50): Promise<T> => {
    try {
        return await callback();
    } catch (error) {
        if (attempts <= 1) throw error;
        await delay(200);
        return eventually(callback, attempts - 1);
    }
};

describe.sequential('API versions', () => {
    const topicName = randomName('versions');
    let cluster: Cluster;
    let supportedVersions: Record<number, { minVersion: number; maxVersion: number }>;
    let topicId: string;
    let leaderId: number;

    const forEachVersion = <Request, Response>(
        api: Api<Request, Response>,
        test: (api: Api<Request, Response>) => Promise<void>,
    ) =>
        it.for(getVersions(api).map((version) => [`v${version.apiVersion}`, version] as const))(
            '%s',
            async ([, version], { skip }) => {
                const { minVersion, maxVersion } = supportedVersions[version.apiKey] ?? {
                    minVersion: 0,
                    maxVersion: -1,
                };
                if (version.apiVersion < minVersion || version.apiVersion > maxVersion) {
                    skip(`broker supports versions ${minVersion}-${maxVersion}`);
                }
                await test(version);
            },
        );

    const createTopic = async (name: string) => {
        await cluster.sendRequest(API.CREATE_TOPICS, {
            topics: [{ name, numPartitions: 1, replicationFactor: 3 }],
        });
        return eventually(async () => {
            const { topics } = await cluster.sendRequest(API.METADATA, { topics: [{ id: null, name }] });
            return { topicId: topics[0].topicId, leaderId: topics[0].partitions[0].leaderId };
        });
    };

    const createPartitionData = (value: string) => {
        const now = BigInt(Date.now());
        return {
            index: 0,
            baseOffset: 0n,
            partitionLeaderEpoch: -1,
            attributes: 0,
            lastOffsetDelta: 0,
            baseTimestamp: now,
            maxTimestamp: now,
            producerId: -1n,
            producerEpoch: -1,
            baseSequence: -1,
            records: [
                {
                    attributes: 0,
                    timestampDelta: 0n,
                    offsetDelta: 0,
                    key: 'key',
                    value,
                    headers: [{ key: 'header-key', value: 'header-value' }],
                },
            ],
        };
    };

    const findCoordinator = async (groupId: string) => {
        const { coordinators } = await eventually(() =>
            cluster.sendRequest(API.FIND_COORDINATOR, { keyType: KEY_TYPE.GROUP, keys: [groupId] }),
        );
        return cluster.sendRequestToNode(coordinators[0].nodeId);
    };

    const joinGroup = async (groupId: string, joinGroupApi = API.JOIN_GROUP) => {
        const sendRequest = await findCoordinator(groupId);
        const request = {
            groupId,
            sessionTimeoutMs: 30_000,
            rebalanceTimeoutMs: 60_000,
            memberId: '',
            groupInstanceId: null,
            protocolType: 'consumer',
            protocols: [{ name: 'RoundRobinAssigner', metadata: { version: 0, topics: [topicName] } }],
            reason: null,
        };
        const response = await sendRequest(joinGroupApi, request).catch((error) => {
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.MEMBER_ID_REQUIRED) {
                return sendRequest(joinGroupApi, { ...request, memberId: error.response.memberId });
            }
            throw error;
        });
        return { sendRequest, response };
    };

    const joinAndSyncGroup = async (groupId: string) => {
        const { sendRequest, response } = await joinGroup(groupId);
        const { memberId, generationId } = response;
        await sendRequest(API.SYNC_GROUP, {
            groupId,
            generationId,
            memberId,
            groupInstanceId: null,
            protocolType: 'consumer',
            protocolName: 'RoundRobinAssigner',
            assignments: [{ memberId, assignment: { [topicName]: [0] } }],
        });
        return { sendRequest, memberId, generationId };
    };

    const leaveGroup = async (groupId: string, memberId: string) => {
        const sendRequest = await findCoordinator(groupId);
        await sendRequest(API.LEAVE_GROUP, { groupId, members: [{ memberId, groupInstanceId: null, reason: null }] });
    };

    beforeAll(async () => {
        cluster = kafka.createCluster();
        await cluster.connect();

        const { versions } = await cluster.sendRequest(API.API_VERSIONS, {});
        supportedVersions = Object.fromEntries(versions.map(({ apiKey, ...range }) => [apiKey, range]));

        ({ topicId, leaderId } = await createTopic(topicName));
        await eventually(() =>
            cluster.sendRequestToNode(leaderId)(API.PRODUCE, {
                transactionalId: null,
                acks: -1,
                timeoutMs: 10_000,
                topicData: [{ name: topicName, topicId, partitionData: [createPartitionData('initial')] }],
            }),
        );
    });

    afterAll(async () => {
        await cluster.sendRequest(API.DELETE_TOPICS, { topics: [{ name: topicName, topicId: null }] });
        await cluster.disconnect();
    });

    describe('ApiVersions', () => {
        forEachVersion(API.API_VERSIONS, async (api) => {
            const { versions } = await cluster.sendRequest(api, {});
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
            const { topics, brokers } = await cluster.sendRequest(api, {
                topics: [{ id: null, name: topicName }],
            });
            expect(brokers.length).toBeGreaterThan(0);
            expect(topics).toEqual([
                expect.objectContaining({
                    name: topicName,
                    topicId: api.apiVersion >= 10 ? topicId : '',
                    partitions: [expect.objectContaining({ partitionIndex: 0, leaderId })],
                }),
            ]);
        });
    });

    describe('CreateTopics', () => {
        forEachVersion(API.CREATE_TOPICS, async (api) => {
            const name = randomName(`create-v${api.apiVersion}`);
            const { topics } = await cluster.sendRequest(api, {
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
            const { responses } = await cluster.sendRequest(api, { topics: [{ name, topicId: null }] });
            expect(responses).toEqual([expect.objectContaining({ name, errorCode: 0 })]);
        });
    });

    describe('Produce', () => {
        forEachVersion(API.PRODUCE, async (api) => {
            const { responses } = await cluster.sendRequestToNode(leaderId)(api, {
                transactionalId: null,
                acks: -1,
                timeoutMs: 10_000,
                topicData: [{ name: topicName, topicId, partitionData: [createPartitionData(`v${api.apiVersion}`)] }],
            });
            expect(responses).toEqual([
                expect.objectContaining(api.apiVersion >= 13 ? { topicId } : { name: topicName }),
            ]);
            expect(responses[0].partitionResponses[0].baseOffset).toBeGreaterThan(0n);
        });
    });

    describe('ListOffsets', () => {
        forEachVersion(API.LIST_OFFSETS, async (api) => {
            const { topics } = await cluster.sendRequestToNode(leaderId)(api, {
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
            const { responses } = await cluster.sendRequestToNode(leaderId)(api, {
                maxWaitMs: 100,
                minBytes: 1,
                maxBytes: 1_048_576,
                isolationLevel: 0,
                sessionId: 0,
                sessionEpoch: -1,
                topics: [
                    {
                        topicId,
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
            expect(responses).toEqual([expect.objectContaining(api.apiVersion >= 13 ? { topicId } : { topicName })]);
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
            const result = await cluster.sendRequest(api, {
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
                cluster.sendRequest(api, { keyType: KEY_TYPE.GROUP, keys: [randomName('group')] }),
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
                        topicId,
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
            expect(topics).toEqual([expect.objectContaining(api.apiVersion >= 10 ? { topicId } : { name: topicName })]);

            const { groups } = await sendRequest(API.OFFSET_FETCH, {
                groups: [{ groupId, topics: [{ name: topicName, topicId, partitionIndexes: [0] }] }],
                requireStable: false,
            });
            expect(groups[0].topics[0].partitions[0].committedOffset).toBe(BigInt(api.apiVersion));
        });
    });

    describe('OffsetFetch', () => {
        forEachVersion(API.OFFSET_FETCH, async (api) => {
            const groupId = randomName(`fetch-offsets-v${api.apiVersion}`);
            const sendRequest = await findCoordinator(groupId);
            await sendRequest(API.OFFSET_COMMIT, {
                groupId,
                generationIdOrMemberEpoch: -1,
                memberId: '',
                groupInstanceId: null,
                topics: [
                    {
                        name: topicName,
                        topicId,
                        partitions: [
                            {
                                partitionIndex: 0,
                                committedOffset: 1n,
                                committedLeaderEpoch: -1,
                                committedMetadata: 'meta',
                            },
                        ],
                    },
                ],
            });

            const { groups } = await sendRequest(api, {
                groups: [{ groupId, topics: [{ name: topicName, topicId, partitionIndexes: [0] }] }],
                requireStable: true,
            });
            expect(groups[0].topics).toEqual([
                expect.objectContaining(api.apiVersion >= 10 ? { topicId } : { name: topicName }),
            ]);
            expect(groups[0].topics[0].partitions[0]).toMatchObject({
                partitionIndex: 0,
                committedOffset: 1n,
                committedMetadata: 'meta',
            });
        });
    });
});
