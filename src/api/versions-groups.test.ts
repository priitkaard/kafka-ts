import { randomBytes } from 'crypto';
import { describe, expect } from 'vitest';
import { API, API_ERROR } from '.';
import { createVersionTestContext, eventually, randomName } from './versions.test-utils';

const RESOURCE_TYPE_GROUP = 32;
const KEY_TYPE_SHARE = 2;
const ACKNOWLEDGE_TYPE_ACCEPT = 1;

const randomMemberId = () => randomBytes(16).toString('base64url');

describe.sequential('Group API versions', () => {
    const context = createVersionTestContext('groups');
    const { topicName, forEachVersion, findCoordinator, joinAndSyncGroup, leaveGroup, commitOffset } = context;

    describe('DescribeGroups', () => {
        forEachVersion(API.DESCRIBE_GROUPS, async (api) => {
            const groupId = randomName(`describe-v${api.apiVersion}`);
            const { sendRequest, memberId } = await joinAndSyncGroup(groupId);
            const { groups } = await sendRequest(api, { groups: [groupId], includeAuthorizedOperations: true });
            expect(groups).toEqual([
                expect.objectContaining({
                    groupId,
                    groupState: 'Stable',
                    protocolType: 'consumer',
                    members: [expect.objectContaining({ memberId })],
                }),
            ]);
            await leaveGroup(groupId, memberId);
        });
    });

    describe('ListGroups', () => {
        forEachVersion(API.LIST_GROUPS, async (api) => {
            const groupId = randomName(`list-v${api.apiVersion}`);
            const sendRequest = await commitOffset(groupId);
            const { groups } = await sendRequest(api, { statesFilter: [], typesFilter: [] });
            expect(groups).toContainEqual(expect.objectContaining({ groupId }));
        });
    });

    describe('DeleteGroups', () => {
        forEachVersion(API.DELETE_GROUPS, async (api) => {
            const groupId = randomName(`delete-v${api.apiVersion}`);
            const sendRequest = await commitOffset(groupId);
            const { results } = await sendRequest(api, { groupsNames: [groupId] });
            expect(results).toEqual([expect.objectContaining({ groupId, errorCode: 0 })]);
        });
    });

    describe('OffsetDelete', () => {
        forEachVersion(API.OFFSET_DELETE, async (api) => {
            const groupId = randomName(`offset-delete-v${api.apiVersion}`);
            const sendRequest = await commitOffset(groupId);
            const { topics } = await sendRequest(api, {
                groupId,
                topics: [{ name: topicName, partitions: [{ partitionIndex: 0 }] }],
            });
            expect(topics).toEqual([expect.objectContaining({ name: topicName })]);
        });
    });

    describe('Consumer groups', () => {
        const heartbeat = async (groupId: string, api = API.CONSUMER_GROUP_HEARTBEAT) => {
            const sendRequest = await findCoordinator(groupId);
            const response = await sendRequest(api, {
                groupId,
                memberId: api.apiVersion === 0 ? '' : randomMemberId(),
                memberEpoch: 0,
                instanceId: null,
                rackId: null,
                rebalanceTimeoutMs: 30_000,
                subscribedTopicNames: [topicName],
                subscribedTopicRegex: null,
                serverAssignor: null,
                topicPartitions: [],
            });
            return { sendRequest, response };
        };

        const leave = async (groupId: string, memberId: string) => {
            const sendRequest = await findCoordinator(groupId);
            await sendRequest(API.CONSUMER_GROUP_HEARTBEAT, {
                groupId,
                memberId,
                memberEpoch: -1,
                instanceId: null,
                rackId: null,
                rebalanceTimeoutMs: -1,
                subscribedTopicNames: null,
                serverAssignor: null,
                topicPartitions: null,
            });
        };

        describe('ConsumerGroupHeartbeat', () => {
            forEachVersion(API.CONSUMER_GROUP_HEARTBEAT, async (api, skipIfDisabled) => {
                const groupId = randomName(`consumer-heartbeat-v${api.apiVersion}`);
                const { response } = await heartbeat(groupId, api).catch(skipIfDisabled);
                expect(response.memberId).toBeTruthy();
                expect(response.memberEpoch).toBeGreaterThan(0);
                await leave(groupId, response.memberId!);
            });
        });

        describe('ConsumerGroupDescribe', () => {
            forEachVersion(API.CONSUMER_GROUP_DESCRIBE, async (api, skipIfDisabled) => {
                const groupId = randomName(`consumer-describe-v${api.apiVersion}`);
                const { sendRequest, response } = await heartbeat(groupId).catch(skipIfDisabled);
                const { groups } = await sendRequest(api, { groupIds: [groupId], includeAuthorizedOperations: true });
                expect(groups).toEqual([
                    expect.objectContaining({
                        groupId,
                        members: [
                            expect.objectContaining({ memberId: response.memberId, subscribedTopicNames: [topicName] }),
                        ],
                    }),
                ]);
                await leave(groupId, response.memberId!);
            });
        });
    });

    describe('Share groups', () => {
        const joinShareGroup = async (groupId: string, api = API.SHARE_GROUP_HEARTBEAT) => {
            await context.cluster.sendRequest(API.INCREMENTAL_ALTER_CONFIGS, {
                resources: [
                    {
                        resourceType: RESOURCE_TYPE_GROUP,
                        resourceName: groupId,
                        configs: [{ name: 'share.auto.offset.reset', configOperation: 0, value: 'earliest' }],
                    },
                ],
                validateOnly: false,
            });
            const sendRequest = await findCoordinator(groupId);
            const memberId = randomMemberId();
            const request = { groupId, memberId, rackId: null, subscribedTopicNames: [topicName] };
            const response = await sendRequest(api, { ...request, memberEpoch: 0 });
            await eventually(async () => {
                const { assignment } = await sendRequest(API.SHARE_GROUP_HEARTBEAT, {
                    ...request,
                    memberEpoch: response.memberEpoch,
                });
                expect(assignment?.topicPartitions).toEqual([expect.objectContaining({ topicId: context.topicId })]);
            });
            return { sendRequest, memberId, response };
        };

        const leaveShareGroup = async (groupId: string, memberId: string) => {
            const sendRequest = await findCoordinator(groupId);
            await sendRequest(API.SHARE_GROUP_HEARTBEAT, {
                groupId,
                memberId,
                memberEpoch: -1,
                rackId: null,
                subscribedTopicNames: null,
            });
        };

        const shareFetch = (groupId: string, memberId: string, api = API.SHARE_FETCH) =>
            eventually(async () => {
                const response = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                    groupId,
                    memberId,
                    shareSessionEpoch: 0,
                    maxWaitMs: 500,
                    minBytes: 1,
                    maxBytes: 1_048_576,
                    maxRecords: 500,
                    batchSize: 500,
                    topics: [
                        { topicId: context.topicId, partitions: [{ partitionIndex: 0, acknowledgementBatches: [] }] },
                    ],
                    forgottenTopicsData: [],
                });
                expect(response.responses[0].partitions[0].acquiredRecords.length).toBeGreaterThan(0);
                return response;
            });

        describe('ShareGroupHeartbeat', () => {
            forEachVersion(API.SHARE_GROUP_HEARTBEAT, async (api, skipIfDisabled) => {
                const groupId = randomName(`share-heartbeat-v${api.apiVersion}`);
                const { memberId, response } = await joinShareGroup(groupId, api).catch(skipIfDisabled);
                expect(response).toMatchObject({ memberId, errorCode: 0 });
                await leaveShareGroup(groupId, memberId);
            });
        });

        describe('ShareGroupDescribe', () => {
            forEachVersion(API.SHARE_GROUP_DESCRIBE, async (api, skipIfDisabled) => {
                const groupId = randomName(`share-describe-v${api.apiVersion}`);
                const { sendRequest, memberId } = await joinShareGroup(groupId).catch(skipIfDisabled);
                const { groups } = await sendRequest(api, { groupIds: [groupId], includeAuthorizedOperations: true });
                expect(groups).toEqual([
                    expect.objectContaining({ groupId, members: [expect.objectContaining({ memberId })] }),
                ]);
                await leaveShareGroup(groupId, memberId);
            });
        });

        describe('ShareFetch', () => {
            forEachVersion(API.SHARE_FETCH, async (api, skipIfDisabled) => {
                const groupId = randomName(`share-fetch-v${api.apiVersion}`);
                const { memberId } = await joinShareGroup(groupId).catch(skipIfDisabled);
                const { responses } = await shareFetch(groupId, memberId, api);
                expect(responses[0]).toMatchObject({ topicId: context.topicId });
                expect(responses[0].partitions[0].records[0].records[0]).toMatchObject({
                    key: 'key',
                    value: 'initial',
                });
                await leaveShareGroup(groupId, memberId);
            });
        });

        describe('ShareAcknowledge', () => {
            forEachVersion(API.SHARE_ACKNOWLEDGE, async (api, skipIfDisabled) => {
                const groupId = randomName(`share-acknowledge-v${api.apiVersion}`);
                const { memberId } = await joinShareGroup(groupId).catch(skipIfDisabled);
                const { responses } = await shareFetch(groupId, memberId);
                const [{ firstOffset, lastOffset }] = responses[0].partitions[0].acquiredRecords;
                const result = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                    groupId,
                    memberId,
                    shareSessionEpoch: 1,
                    topics: [
                        {
                            topicId: context.topicId,
                            partitions: [
                                {
                                    partitionIndex: 0,
                                    acknowledgementBatches: [
                                        { firstOffset, lastOffset, acknowledgeTypes: [ACKNOWLEDGE_TYPE_ACCEPT] },
                                    ],
                                },
                            ],
                        },
                    ],
                });
                expect(result.responses[0].partitions[0]).toMatchObject({ partitionIndex: 0, errorCode: 0 });
                await leaveShareGroup(groupId, memberId);
            });
        });

        describe('DescribeShareGroupOffsets', () => {
            forEachVersion(API.DESCRIBE_SHARE_GROUP_OFFSETS, async (api, skipIfDisabled) => {
                const groupId = randomName(`share-offsets-v${api.apiVersion}`);
                const { sendRequest, memberId } = await joinShareGroup(groupId).catch(skipIfDisabled);
                await shareFetch(groupId, memberId);
                const { groups } = await sendRequest(api, {
                    groups: [{ groupId, topics: [{ topicName, partitions: [0] }] }],
                });
                expect(groups).toEqual([
                    expect.objectContaining({ groupId, topics: [expect.objectContaining({ topicName })] }),
                ]);
                await leaveShareGroup(groupId, memberId);
            });
        });

        describe('AlterShareGroupOffsets', () => {
            forEachVersion(API.ALTER_SHARE_GROUP_OFFSETS, async (api, skipIfDisabled) => {
                const groupId = randomName(`share-alter-v${api.apiVersion}`);
                const { sendRequest, memberId } = await joinShareGroup(groupId).catch(skipIfDisabled);
                await leaveShareGroup(groupId, memberId);
                const { responses } = await sendRequest(api, {
                    groupId,
                    topics: [{ topicName, partitions: [{ partitionIndex: 0, startOffset: 0n }] }],
                });
                expect(responses).toEqual([expect.objectContaining({ topicName, topicId: context.topicId })]);
            });
        });

        describe('DeleteShareGroupOffsets', () => {
            forEachVersion(API.DELETE_SHARE_GROUP_OFFSETS, async (api, skipIfDisabled) => {
                const groupId = randomName(`share-delete-v${api.apiVersion}`);
                const { sendRequest, memberId } = await joinShareGroup(groupId).catch(skipIfDisabled);
                await shareFetch(groupId, memberId);
                await leaveShareGroup(groupId, memberId);
                const { responses } = await sendRequest(api, { groupId, topics: [{ topicName }] });
                expect(responses).toEqual([expect.objectContaining({ topicName, errorCode: 0 })]);
            });
        });

        describe('Share group state', () => {
            const findShareCoordinator = (groupId: string) =>
                findCoordinator(
                    `${groupId}:${Buffer.from(context.topicId, 'hex').toString('base64url')}:0`,
                    KEY_TYPE_SHARE,
                );

            const initialize = async (groupId: string, api = API.INITIALIZE_SHARE_GROUP_STATE) => {
                const sendRequest = await findShareCoordinator(groupId);
                const response = await sendRequest(api, {
                    groupId,
                    topics: [
                        { topicId: context.topicId, partitions: [{ partition: 0, stateEpoch: 0, startOffset: 0n }] },
                    ],
                });
                return { sendRequest, response };
            };

            const partitions = [{ partition: 0, leaderEpoch: 0 }];

            describe('InitializeShareGroupState', () => {
                forEachVersion(API.INITIALIZE_SHARE_GROUP_STATE, async (api) => {
                    const { response } = await initialize(randomName(`state-init-v${api.apiVersion}`), api);
                    expect(response.results[0].partitions).toEqual([
                        expect.objectContaining({ partition: 0, errorCode: 0 }),
                    ]);
                });
            });

            describe('ReadShareGroupState', () => {
                forEachVersion(API.READ_SHARE_GROUP_STATE, async (api) => {
                    const groupId = randomName(`state-read-v${api.apiVersion}`);
                    const { sendRequest } = await initialize(groupId);
                    const { results } = await sendRequest(api, {
                        groupId,
                        topics: [{ topicId: context.topicId, partitions }],
                    });
                    expect(results[0].partitions).toEqual([expect.objectContaining({ partition: 0, startOffset: 0n })]);
                });
            });

            describe('ReadShareGroupStateSummary', () => {
                forEachVersion(API.READ_SHARE_GROUP_STATE_SUMMARY, async (api) => {
                    const groupId = randomName(`state-summary-v${api.apiVersion}`);
                    const { sendRequest } = await initialize(groupId);
                    const { results } = await sendRequest(api, {
                        groupId,
                        topics: [{ topicId: context.topicId, partitions }],
                    });
                    expect(results[0].partitions).toEqual([expect.objectContaining({ partition: 0, startOffset: 0n })]);
                });
            });

            describe('WriteShareGroupState', () => {
                forEachVersion(API.WRITE_SHARE_GROUP_STATE, async (api) => {
                    const groupId = randomName(`state-write-v${api.apiVersion}`);
                    const { sendRequest } = await initialize(groupId);
                    const { results } = await sendRequest(api, {
                        groupId,
                        topics: [
                            {
                                topicId: context.topicId,
                                partitions: [
                                    {
                                        partition: 0,
                                        stateEpoch: 0,
                                        leaderEpoch: 0,
                                        startOffset: 1n,
                                        deliveryCompleteCount: 0,
                                        stateBatches: [],
                                    },
                                ],
                            },
                        ],
                    });
                    expect(results[0].partitions).toEqual([expect.objectContaining({ partition: 0, errorCode: 0 })]);
                });
            });

            describe('DeleteShareGroupState', () => {
                forEachVersion(API.DELETE_SHARE_GROUP_STATE, async (api) => {
                    const groupId = randomName(`state-delete-v${api.apiVersion}`);
                    const { sendRequest } = await initialize(groupId);
                    const { results } = await sendRequest(api, {
                        groupId,
                        topics: [{ topicId: context.topicId, partitions: [{ partition: 0 }] }],
                    });
                    expect(results[0].partitions).toEqual([expect.objectContaining({ partition: 0, errorCode: 0 })]);
                });
            });
        });
    });

    describe('Streams groups', () => {
        const heartbeat = async (groupId: string, api = API.STREAMS_GROUP_HEARTBEAT) => {
            const sendRequest = await findCoordinator(groupId);
            const response = await sendRequest(api, {
                groupId,
                memberId: randomMemberId(),
                memberEpoch: 0,
                endpointInformationEpoch: 0,
                instanceId: null,
                rackId: null,
                rebalanceTimeoutMs: 30_000,
                topology: {
                    epoch: 0,
                    subtopologies: [
                        {
                            subtopologyId: '0',
                            sourceTopics: [topicName],
                            sourceTopicRegex: [],
                            stateChangelogTopics: [],
                            repartitionSinkTopics: [],
                            repartitionSourceTopics: [],
                            copartitionGroups: [],
                        },
                    ],
                },
                activeTasks: [],
                standbyTasks: [],
                warmupTasks: [],
                processId: randomMemberId(),
                userEndpoint: null,
                clientTags: null,
                taskOffsets: null,
                taskEndOffsets: null,
                shutdownApplication: false,
            });
            return { sendRequest, response };
        };

        describe('StreamsGroupHeartbeat', () => {
            forEachVersion(API.STREAMS_GROUP_HEARTBEAT, async (api) => {
                const { response } = await heartbeat(randomName(`streams-heartbeat-v${api.apiVersion}`), api);
                expect(response.memberEpoch).toBeGreaterThan(0);
            });
        });

        describe('StreamsGroupDescribe', () => {
            forEachVersion(API.STREAMS_GROUP_DESCRIBE, async (api) => {
                const groupId = randomName(`streams-describe-v${api.apiVersion}`);
                const { sendRequest, response } = await heartbeat(groupId);
                const { groups } = await sendRequest(api, { groupIds: [groupId], includeAuthorizedOperations: true });
                expect(groups).toEqual([
                    expect.objectContaining({
                        groupId,
                        members: [expect.objectContaining({ memberId: response.memberId })],
                    }),
                ]);
            });
        });
    });

    describe('Errors', () => {
        forEachVersion(API.DESCRIBE_GROUPS, async (api) => {
            const sendRequest = await findCoordinator('unknown-group');
            const { groups } = await sendRequest(api, { groups: ['unknown-group'] }).catch((error) => error.response);
            expect(groups[0].errorCode).toBe(api.apiVersion >= 6 ? API_ERROR.GROUP_ID_NOT_FOUND : 0);
        });
    });
});
