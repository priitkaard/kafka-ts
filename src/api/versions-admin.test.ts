import { randomBytes } from 'crypto';
import { describe, expect } from 'vitest';
import { API, API_ERROR } from '.';
import { createVersionTestContext, eventually, expectApiError, randomName } from './versions.test-utils';

const RESOURCE_TYPE = { ANY: 1, TOPIC: 2, BROKER: 4 };
const PATTERN_TYPE = { ANY: 1, LITERAL: 3 };
const ACL_OPERATION = { ANY: 1, READ: 3 };
const ACL_PERMISSION = { ANY: 1, ALLOW: 3 };
const SCRAM_SHA_256 = 1;
const LOG_DIR = '/tmp/kraft-combined-logs';

describe.sequential('Admin API versions', () => {
    const context = createVersionTestContext('admin');
    const { topicName, forEachVersion } = context;

    describe('DescribeConfigs', () => {
        forEachVersion(API.DESCRIBE_CONFIGS, async (api) => {
            const { results } = await context.cluster.sendRequest(api, {
                resources: [{ resourceType: RESOURCE_TYPE.TOPIC, resourceName: topicName, configurationKeys: null }],
                includeSynonyms: true,
                includeDocumentation: true,
            });
            expect(results[0].resourceName).toBe(topicName);
            expect(results[0].configs).toContainEqual(expect.objectContaining({ name: 'cleanup.policy' }));
        });
    });

    describe('AlterConfigs', () => {
        forEachVersion(API.ALTER_CONFIGS, async (api) => {
            const { responses } = await context.cluster.sendRequest(api, {
                resources: [
                    {
                        resourceType: RESOURCE_TYPE.TOPIC,
                        resourceName: topicName,
                        configs: [{ name: 'retention.ms', value: '86400000' }],
                    },
                ],
                validateOnly: true,
            });
            expect(responses).toEqual([expect.objectContaining({ resourceName: topicName, errorCode: 0 })]);
        });
    });

    describe('IncrementalAlterConfigs', () => {
        forEachVersion(API.INCREMENTAL_ALTER_CONFIGS, async (api) => {
            const { responses } = await context.cluster.sendRequest(api, {
                resources: [
                    {
                        resourceType: RESOURCE_TYPE.TOPIC,
                        resourceName: topicName,
                        configs: [{ name: 'retention.ms', configOperation: 0, value: '86400000' }],
                    },
                ],
                validateOnly: true,
            });
            expect(responses).toEqual([expect.objectContaining({ resourceName: topicName, errorCode: 0 })]);
        });
    });

    describe('ListConfigResources', () => {
        forEachVersion(API.LIST_CONFIG_RESOURCES, async (api) => {
            const { configResources } = await context.cluster.sendRequest(api, { resourceTypes: [] });
            expect(configResources).toBeInstanceOf(Array);
        });
    });

    describe('CreatePartitions', () => {
        forEachVersion(API.CREATE_PARTITIONS, async (api) => {
            const { results } = await context.cluster.sendRequest(api, {
                topics: [{ name: topicName, count: 2, assignments: null }],
                timeoutMs: 10_000,
                validateOnly: true,
            });
            expect(results).toEqual([expect.objectContaining({ name: topicName, errorCode: 0 })]);
        });
    });

    describe('DeleteRecords', () => {
        forEachVersion(API.DELETE_RECORDS, async (api) => {
            const { topics } = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                topics: [{ name: topicName, partitions: [{ partitionIndex: 0, offset: 0n }] }],
                timeoutMs: 10_000,
            });
            expect(topics[0].partitions[0]).toMatchObject({ partitionIndex: 0, lowWatermark: 0n });
        });
    });

    describe('OffsetForLeaderEpoch', () => {
        forEachVersion(API.OFFSET_FOR_LEADER_EPOCH, async (api) => {
            const { topics } = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                replicaId: -1,
                topics: [{ topic: topicName, partitions: [{ partition: 0, currentLeaderEpoch: -1, leaderEpoch: 0 }] }],
            });
            expect(topics[0].partitions[0].endOffset).toBeGreaterThan(0n);
        });
    });

    describe('DescribeLogDirs', () => {
        forEachVersion(API.DESCRIBE_LOG_DIRS, async (api) => {
            const { results } = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                topics: [{ topic: topicName, partitions: [0] }],
            });
            expect(results).toContainEqual(
                expect.objectContaining({
                    logDir: LOG_DIR,
                    topics: [expect.objectContaining({ name: topicName })],
                }),
            );
        });
    });

    describe('AlterReplicaLogDirs', () => {
        forEachVersion(API.ALTER_REPLICA_LOG_DIRS, async (api) => {
            const { results } = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                dirs: [{ path: LOG_DIR, topics: [{ name: topicName, partitions: [0] }] }],
            });
            expect(results).toEqual([expect.objectContaining({ topicName })]);
        });
    });

    describe('ElectLeaders', () => {
        forEachVersion(API.ELECT_LEADERS, async (api) => {
            await expectApiError(
                context.cluster.sendRequest(api, {
                    electionType: 0,
                    topicPartitions: [{ topic: topicName, partitions: [0] }],
                    timeoutMs: 10_000,
                }),
                API_ERROR.ELECTION_NOT_NEEDED,
            );
        });
    });

    describe('AlterPartitionReassignments', () => {
        forEachVersion(API.ALTER_PARTITION_REASSIGNMENTS, async (api) => {
            await expectApiError(
                context.cluster.sendRequest(api, {
                    timeoutMs: 10_000,
                    topics: [{ name: topicName, partitions: [{ partitionIndex: 0, replicas: null }] }],
                }),
                API_ERROR.NO_REASSIGNMENT_IN_PROGRESS,
            );
        });
    });

    describe('ListPartitionReassignments', () => {
        forEachVersion(API.LIST_PARTITION_REASSIGNMENTS, async (api) => {
            const { topics } = await context.cluster.sendRequest(api, { timeoutMs: 10_000, topics: null });
            expect(topics).toEqual([]);
        });
    });

    describe('DescribeTopicPartitions', () => {
        forEachVersion(API.DESCRIBE_TOPIC_PARTITIONS, async (api) => {
            const { topics } = await context.cluster.sendRequest(api, {
                topics: [{ name: topicName }],
                responsePartitionLimit: 2000,
                cursor: null,
            });
            expect(topics).toEqual([
                expect.objectContaining({
                    name: topicName,
                    topicId: context.topicId,
                    partitions: [expect.objectContaining({ partitionIndex: 0, leaderId: context.leaderId })],
                }),
            ]);
        });
    });

    describe('DescribeCluster', () => {
        forEachVersion(API.DESCRIBE_CLUSTER, async (api) => {
            const { brokers, clusterId } = await context.cluster.sendRequest(api, {
                includeClusterAuthorizedOperations: true,
                endpointType: 1,
            });
            expect(clusterId).toBe('4L6g3nShT-eMCtK--X86sw');
            expect(brokers.map(({ brokerId }) => brokerId).sort()).toEqual([0, 1, 2]);
        });
    });

    describe('DescribeQuorum', () => {
        forEachVersion(API.DESCRIBE_QUORUM, async (api) => {
            const { topics } = await context.cluster.sendRequest(api, {
                topics: [{ topicName: '__cluster_metadata', partitions: [{ partitionIndex: 0 }] }],
            });
            expect(topics[0].partitions[0].currentVoters).toHaveLength(3);
        });
    });

    describe('UpdateFeatures', () => {
        forEachVersion(API.UPDATE_FEATURES, async (api) => {
            const { results } = await context.cluster.sendRequest(api, {
                timeoutMs: 10_000,
                featureUpdates: [],
                validateOnly: true,
            });
            expect(results).toEqual([]);
        });
    });

    describe('UnregisterBroker', () => {
        forEachVersion(API.UNREGISTER_BROKER, async (api) => {
            await expectApiError(
                context.cluster.sendRequest(api, { brokerId: 1000 }),
                API_ERROR.BROKER_ID_NOT_REGISTERED,
            );
        });
    });

    describe('AddRaftVoter', () => {
        forEachVersion(API.ADD_RAFT_VOTER, async (api) => {
            await expectApiError(
                context.cluster.sendRequest(api, {
                    clusterId: '4L6g3nShT-eMCtK--X86sw',
                    timeoutMs: 10_000,
                    voterId: 1000,
                    voterDirectoryId: randomBytes(16).toString('hex'),
                    listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 29099 }],
                    ackWhenCommitted: true,
                }),
                API_ERROR.UNSUPPORTED_VERSION,
            );
        });
    });

    describe('RemoveRaftVoter', () => {
        forEachVersion(API.REMOVE_RAFT_VOTER, async (api) => {
            await expectApiError(
                context.cluster.sendRequest(api, {
                    clusterId: '4L6g3nShT-eMCtK--X86sw',
                    voterId: 1000,
                    voterDirectoryId: randomBytes(16).toString('hex'),
                }),
                API_ERROR.UNSUPPORTED_VERSION,
            );
        });
    });

    describe('DescribeClientQuotas', () => {
        forEachVersion(API.DESCRIBE_CLIENT_QUOTAS, async (api) => {
            const { entries } = await context.cluster.sendRequest(api, {
                components: [{ entityType: 'user', matchType: 2, match: null }],
                strict: false,
            });
            expect(entries).toBeInstanceOf(Array);
        });
    });

    describe('AlterClientQuotas', () => {
        forEachVersion(API.ALTER_CLIENT_QUOTAS, async (api) => {
            const entity = [{ entityType: 'user', entityName: 'kafka-ts' }];
            const { entries } = await context.cluster.sendRequest(api, {
                entries: [{ entity, ops: [{ key: 'producer_byte_rate', value: 1024, remove: false }] }],
                validateOnly: true,
            });
            expect(entries).toEqual([
                expect.objectContaining({ errorCode: 0, entity: [expect.objectContaining(entity[0])] }),
            ]);
        });
    });

    describe('AlterUserScramCredentials', () => {
        forEachVersion(API.ALTER_USER_SCRAM_CREDENTIALS, async (api) => {
            const user = randomName(`scram-v${api.apiVersion}`);
            const { results } = await context.cluster.sendRequest(api, {
                deletions: [],
                upsertions: [
                    {
                        name: user,
                        mechanism: SCRAM_SHA_256,
                        iterations: 4096,
                        salt: randomBytes(16),
                        saltedPassword: randomBytes(32),
                    },
                ],
            });
            expect(results).toEqual([expect.objectContaining({ user, errorCode: 0 })]);
        });
    });

    describe('DescribeUserScramCredentials', () => {
        forEachVersion(API.DESCRIBE_USER_SCRAM_CREDENTIALS, async (api) => {
            const user = randomName(`scram-describe-v${api.apiVersion}`);
            await context.cluster.sendRequest(API.ALTER_USER_SCRAM_CREDENTIALS, {
                deletions: [],
                upsertions: [
                    {
                        name: user,
                        mechanism: SCRAM_SHA_256,
                        iterations: 4096,
                        salt: randomBytes(16),
                        saltedPassword: randomBytes(32),
                    },
                ],
            });
            await eventually(async () => {
                const { results } = await context.cluster.sendRequest(api, { users: [{ name: user }] });
                expect(results).toEqual([
                    expect.objectContaining({
                        user,
                        credentialInfos: [{ mechanism: SCRAM_SHA_256, iterations: 4096, tags: {} }],
                    }),
                ]);
            });
        });
    });

    describe('ACLs', () => {
        const acl = (principal: string) => ({
            resourceType: RESOURCE_TYPE.TOPIC,
            resourceName: topicName,
            resourcePatternType: PATTERN_TYPE.LITERAL,
            principal,
            host: '*',
            operation: ACL_OPERATION.READ,
            permissionType: ACL_PERMISSION.ALLOW,
        });
        const filter = (principal: string | null) => ({
            resourceTypeFilter: RESOURCE_TYPE.ANY,
            resourceNameFilter: null,
            patternTypeFilter: PATTERN_TYPE.ANY,
            principalFilter: principal,
            hostFilter: null,
            operation: ACL_OPERATION.ANY,
            permissionType: ACL_PERMISSION.ANY,
        });

        describe('CreateAcls', () => {
            forEachVersion(API.CREATE_ACLS, async (api) => {
                const { results } = await context.cluster.sendRequest(api, {
                    creations: [acl(`User:${randomName(`create-v${api.apiVersion}`)}`)],
                });
                expect(results).toEqual([expect.objectContaining({ errorCode: 0 })]);
            });
        });

        describe('DescribeAcls', () => {
            forEachVersion(API.DESCRIBE_ACLS, async (api) => {
                const principal = `User:${randomName(`describe-v${api.apiVersion}`)}`;
                await context.cluster.sendRequest(API.CREATE_ACLS, { creations: [acl(principal)] });
                await eventually(async () => {
                    const { resources } = await context.cluster.sendRequest(api, filter(principal));
                    expect(resources).toEqual([
                        expect.objectContaining({
                            resourceName: topicName,
                            acls: [expect.objectContaining({ principal })],
                        }),
                    ]);
                });
            });
        });

        describe('DeleteAcls', () => {
            forEachVersion(API.DELETE_ACLS, async (api) => {
                const principal = `User:${randomName(`delete-v${api.apiVersion}`)}`;
                await context.cluster.sendRequest(API.CREATE_ACLS, { creations: [acl(principal)] });
                const { filterResults } = await context.cluster.sendRequest(api, { filters: [filter(principal)] });
                expect(filterResults[0].matchingAcls).toEqual([expect.objectContaining({ principal })]);
            });
        });
    });

    describe('Delegation tokens', () => {
        const createToken = () =>
            context.cluster.sendRequest(API.CREATE_DELEGATION_TOKEN, { renewers: [], maxLifetimeMs: -1n });

        describe('CreateDelegationToken', () => {
            forEachVersion(API.CREATE_DELEGATION_TOKEN, async (api) => {
                const { principalName, hmac } = await context.cluster.sendRequest(api, {
                    renewers: [{ principalType: 'User', principalName: 'admin' }],
                    maxLifetimeMs: -1n,
                });
                expect(principalName).toBe('admin');
                expect(hmac.length).toBeGreaterThan(0);
            });
        });

        describe('DescribeDelegationToken', () => {
            forEachVersion(API.DESCRIBE_DELEGATION_TOKEN, async (api) => {
                const { tokenId } = await createToken();
                await eventually(async () => {
                    const { tokens } = await context.cluster.sendRequest(api, { owners: null });
                    expect(tokens).toContainEqual(expect.objectContaining({ tokenId, principalName: 'admin' }));
                });
            });
        });

        describe('RenewDelegationToken', () => {
            forEachVersion(API.RENEW_DELEGATION_TOKEN, async (api) => {
                const { hmac } = await createToken();
                const { expiryTimestampMs } = await eventually(() =>
                    context.cluster.sendRequest(api, { hmac, renewPeriodMs: -1n }),
                );
                expect(expiryTimestampMs).toBeGreaterThan(0n);
            });
        });

        describe('ExpireDelegationToken', () => {
            forEachVersion(API.EXPIRE_DELEGATION_TOKEN, async (api) => {
                const { hmac } = await createToken();
                const { expiryTimestampMs } = await eventually(() =>
                    context.cluster.sendRequest(api, { hmac, expiryTimePeriodMs: 60_000n }),
                );
                expect(expiryTimestampMs).toBeGreaterThan(0n);
            });
        });
    });

    describe('Client telemetry', () => {
        describe('GetTelemetrySubscriptions', () => {
            forEachVersion(API.GET_TELEMETRY_SUBSCRIPTIONS, async (api) => {
                const { clientInstanceId } = await context.cluster.sendRequest(api, { clientInstanceId: null });
                expect(clientInstanceId).toMatch(/^[0-9a-f]{32}$/);
            });
        });

        describe('PushTelemetry', () => {
            forEachVersion(API.PUSH_TELEMETRY, async (api) => {
                await expectApiError(
                    context.cluster.sendRequest(api, {
                        clientInstanceId: randomBytes(16).toString('hex'),
                        subscriptionId: 0,
                        terminating: false,
                        compressionType: 0,
                        metrics: Buffer.alloc(0),
                    }),
                    API_ERROR.UNKNOWN_SUBSCRIPTION_ID,
                );
            });
        });
    });
});
