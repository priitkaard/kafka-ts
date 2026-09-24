import { randomBytes } from 'crypto';
import { readFileSync } from 'fs';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { API, API_ERROR } from '.';
import { saslPlain } from '../auth';
import { createKafkaClient } from '../client';
import { Cluster } from '../cluster';
import { Api } from '../utils/api';
import { delay } from '../utils/delay';
import { KafkaTSApiError } from '../utils/error';
import { KEY_TYPE } from './find-coordinator';

export const connectionOptions = { host: 'localhost', port: 39092 };
export const ssl = { ca: readFileSync('./certs/ca.crt').toString() };

const kafka = createKafkaClient({
    clientId: 'kafka-ts',
    bootstrapServers: [connectionOptions],
    sasl: saslPlain({ username: 'admin', password: 'admin' }),
    ssl,
});

export const getVersions = <Request, Response>(api: Api<Request, Response>) => {
    const versions: Api<Request, Response>[] = [];
    for (let version: Api<Request, Response> | undefined = api; version; version = version.fallback) {
        versions.unshift(version);
    }
    return versions;
};

export const randomName = (prefix: string) => `kafka-ts-${prefix}-${randomBytes(6).toString('hex')}`;

export const eventually = async <T>(callback: () => Promise<T>, attempts = 50): Promise<T> => {
    try {
        return await callback();
    } catch (error) {
        if (attempts <= 1) throw error;
        await delay(200);
        return eventually(callback, attempts - 1);
    }
};

const COORDINATOR_ERRORS: number[] = [
    API_ERROR.COORDINATOR_LOAD_IN_PROGRESS,
    API_ERROR.COORDINATOR_NOT_AVAILABLE,
    API_ERROR.NOT_COORDINATOR,
];

const retryOnCoordinatorError = async <T>(callback: () => Promise<T>, attempts = 100): Promise<T> => {
    try {
        return await callback();
    } catch (error) {
        const isCoordinatorError = error instanceof KafkaTSApiError && COORDINATOR_ERRORS.includes(error.errorCode);
        if (!isCoordinatorError || attempts <= 1) throw error;
        await delay(200);
        return retryOnCoordinatorError(callback, attempts - 1);
    }
};

export const expectApiError = async (request: Promise<unknown>, ...errorCodes: number[]) => {
    const error = await request.then(
        () => null,
        (error) => error,
    );
    expect(error).toBeInstanceOf(KafkaTSApiError);
    expect(errorCodes).toContain((error as KafkaTSApiError).errorCode);
    return error as KafkaTSApiError;
};

export const createPartitionData = (value: string) => {
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

export const createVersionTestContext = (prefix: string) => {
    const topicName = randomName(prefix);
    let cluster: Cluster;
    let supportedVersions: Record<number, { minVersion: number; maxVersion: number }>;
    let topicId: string;
    let leaderId: number;

    const forEachVersion = <Request, Response>(
        api: Api<Request, Response>,
        test: (api: Api<Request, Response>, skipIfDisabled: (error: unknown) => never) => Promise<void>,
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
                await test(version, (error) => {
                    if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.UNSUPPORTED_VERSION) {
                        skip('feature is not enabled on the broker');
                    }
                    throw error;
                });
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

    const findCoordinator = async (key: string, keyType = KEY_TYPE.GROUP) => {
        const getCoordinator = async () => {
            const { coordinators } = await eventually(() =>
                cluster.sendRequest(API.FIND_COORDINATOR, { keyType, keys: [key] }),
            );
            return cluster.sendRequestToNode(coordinators[0].nodeId);
        };
        const sendRequest = await getCoordinator();
        return ((api, body) =>
            retryOnCoordinatorError(async () => (await getCoordinator())(api, body))) as typeof sendRequest;
    };

    const joinGroup = (groupId: string, joinGroupApi = API.JOIN_GROUP) =>
        eventually(async () => {
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
        });

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

    const commitOffset = async (groupId: string, committedOffset = 1n) => {
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
                        { partitionIndex: 0, committedOffset, committedLeaderEpoch: -1, committedMetadata: 'meta' },
                    ],
                },
            ],
        });
        return sendRequest;
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

    return {
        topicName,
        get cluster() {
            return cluster;
        },
        get topicId() {
            return topicId;
        },
        get leaderId() {
            return leaderId;
        },
        forEachVersion,
        createTopic,
        findCoordinator,
        joinGroup,
        joinAndSyncGroup,
        leaveGroup,
        commitOffset,
    };
};
