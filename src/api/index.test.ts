import { describe, expect, it } from 'vitest';
import { API, API_ERROR } from '.';
import { Api } from '../utils/api';
import { Decoder } from '../utils/decoder';
import { Encoder } from '../utils/encoder';
import { KafkaTSApiError } from '../utils/error';

const getVersions = (api: Api<unknown, unknown>) => {
    const versions: Api<unknown, unknown>[] = [];
    for (let version: Api<unknown, unknown> | undefined = api; version; version = version.fallback) {
        versions.unshift(version);
    }
    return versions;
};

const SPEC: Record<keyof typeof API, { apiKey: number; min: number; max: number; firstFlexible: number | null }> = {
    PRODUCE: { apiKey: 0, min: 3, max: 13, firstFlexible: 9 },
    FETCH: { apiKey: 1, min: 4, max: 18, firstFlexible: 12 },
    LIST_OFFSETS: { apiKey: 2, min: 1, max: 11, firstFlexible: 6 },
    METADATA: { apiKey: 3, min: 0, max: 13, firstFlexible: 9 },
    OFFSET_COMMIT: { apiKey: 8, min: 2, max: 10, firstFlexible: 8 },
    OFFSET_FETCH: { apiKey: 9, min: 1, max: 10, firstFlexible: 6 },
    FIND_COORDINATOR: { apiKey: 10, min: 0, max: 6, firstFlexible: 3 },
    JOIN_GROUP: { apiKey: 11, min: 0, max: 9, firstFlexible: 6 },
    HEARTBEAT: { apiKey: 12, min: 0, max: 4, firstFlexible: 4 },
    LEAVE_GROUP: { apiKey: 13, min: 0, max: 5, firstFlexible: 4 },
    SYNC_GROUP: { apiKey: 14, min: 0, max: 5, firstFlexible: 4 },
    DESCRIBE_GROUPS: { apiKey: 15, min: 0, max: 6, firstFlexible: 5 },
    LIST_GROUPS: { apiKey: 16, min: 0, max: 5, firstFlexible: 3 },
    SASL_HANDSHAKE: { apiKey: 17, min: 0, max: 1, firstFlexible: null },
    API_VERSIONS: { apiKey: 18, min: 0, max: 4, firstFlexible: 3 },
    CREATE_TOPICS: { apiKey: 19, min: 2, max: 7, firstFlexible: 5 },
    DELETE_TOPICS: { apiKey: 20, min: 1, max: 6, firstFlexible: 4 },
    DELETE_RECORDS: { apiKey: 21, min: 0, max: 2, firstFlexible: 2 },
    INIT_PRODUCER_ID: { apiKey: 22, min: 0, max: 6, firstFlexible: 2 },
    OFFSET_FOR_LEADER_EPOCH: { apiKey: 23, min: 2, max: 4, firstFlexible: 4 },
    ADD_PARTITIONS_TO_TXN: { apiKey: 24, min: 0, max: 5, firstFlexible: 3 },
    ADD_OFFSETS_TO_TXN: { apiKey: 25, min: 0, max: 4, firstFlexible: 3 },
    END_TXN: { apiKey: 26, min: 0, max: 5, firstFlexible: 3 },
    WRITE_TXN_MARKERS: { apiKey: 27, min: 1, max: 2, firstFlexible: 1 },
    TXN_OFFSET_COMMIT: { apiKey: 28, min: 0, max: 5, firstFlexible: 3 },
    DESCRIBE_ACLS: { apiKey: 29, min: 1, max: 3, firstFlexible: 2 },
    CREATE_ACLS: { apiKey: 30, min: 1, max: 3, firstFlexible: 2 },
    DELETE_ACLS: { apiKey: 31, min: 1, max: 3, firstFlexible: 2 },
    DESCRIBE_CONFIGS: { apiKey: 32, min: 1, max: 4, firstFlexible: 4 },
    ALTER_CONFIGS: { apiKey: 33, min: 0, max: 2, firstFlexible: 2 },
    ALTER_REPLICA_LOG_DIRS: { apiKey: 34, min: 1, max: 2, firstFlexible: 2 },
    DESCRIBE_LOG_DIRS: { apiKey: 35, min: 1, max: 5, firstFlexible: 2 },
    SASL_AUTHENTICATE: { apiKey: 36, min: 0, max: 2, firstFlexible: 2 },
    CREATE_PARTITIONS: { apiKey: 37, min: 0, max: 3, firstFlexible: 2 },
    CREATE_DELEGATION_TOKEN: { apiKey: 38, min: 1, max: 3, firstFlexible: 2 },
    RENEW_DELEGATION_TOKEN: { apiKey: 39, min: 1, max: 2, firstFlexible: 2 },
    EXPIRE_DELEGATION_TOKEN: { apiKey: 40, min: 1, max: 2, firstFlexible: 2 },
    DESCRIBE_DELEGATION_TOKEN: { apiKey: 41, min: 1, max: 3, firstFlexible: 2 },
    DELETE_GROUPS: { apiKey: 42, min: 0, max: 2, firstFlexible: 2 },
    ELECT_LEADERS: { apiKey: 43, min: 0, max: 2, firstFlexible: 2 },
    INCREMENTAL_ALTER_CONFIGS: { apiKey: 44, min: 0, max: 1, firstFlexible: 1 },
    ALTER_PARTITION_REASSIGNMENTS: { apiKey: 45, min: 0, max: 1, firstFlexible: 0 },
    LIST_PARTITION_REASSIGNMENTS: { apiKey: 46, min: 0, max: 0, firstFlexible: 0 },
    OFFSET_DELETE: { apiKey: 47, min: 0, max: 0, firstFlexible: null },
    DESCRIBE_CLIENT_QUOTAS: { apiKey: 48, min: 0, max: 1, firstFlexible: 1 },
    ALTER_CLIENT_QUOTAS: { apiKey: 49, min: 0, max: 1, firstFlexible: 1 },
    DESCRIBE_USER_SCRAM_CREDENTIALS: { apiKey: 50, min: 0, max: 0, firstFlexible: 0 },
    ALTER_USER_SCRAM_CREDENTIALS: { apiKey: 51, min: 0, max: 0, firstFlexible: 0 },
    DESCRIBE_QUORUM: { apiKey: 55, min: 0, max: 2, firstFlexible: 0 },
    UPDATE_FEATURES: { apiKey: 57, min: 0, max: 2, firstFlexible: 0 },
    DESCRIBE_CLUSTER: { apiKey: 60, min: 0, max: 2, firstFlexible: 0 },
    DESCRIBE_PRODUCERS: { apiKey: 61, min: 0, max: 0, firstFlexible: 0 },
    UNREGISTER_BROKER: { apiKey: 64, min: 0, max: 0, firstFlexible: 0 },
    DESCRIBE_TRANSACTIONS: { apiKey: 65, min: 0, max: 0, firstFlexible: 0 },
    LIST_TRANSACTIONS: { apiKey: 66, min: 0, max: 2, firstFlexible: 0 },
    CONSUMER_GROUP_HEARTBEAT: { apiKey: 68, min: 0, max: 1, firstFlexible: 0 },
    CONSUMER_GROUP_DESCRIBE: { apiKey: 69, min: 0, max: 1, firstFlexible: 0 },
    GET_TELEMETRY_SUBSCRIPTIONS: { apiKey: 71, min: 0, max: 0, firstFlexible: 0 },
    PUSH_TELEMETRY: { apiKey: 72, min: 0, max: 0, firstFlexible: 0 },
    LIST_CONFIG_RESOURCES: { apiKey: 74, min: 0, max: 1, firstFlexible: 0 },
    DESCRIBE_TOPIC_PARTITIONS: { apiKey: 75, min: 0, max: 0, firstFlexible: 0 },
    SHARE_GROUP_HEARTBEAT: { apiKey: 76, min: 1, max: 1, firstFlexible: 1 },
    SHARE_GROUP_DESCRIBE: { apiKey: 77, min: 1, max: 1, firstFlexible: 1 },
    SHARE_FETCH: { apiKey: 78, min: 1, max: 2, firstFlexible: 1 },
    SHARE_ACKNOWLEDGE: { apiKey: 79, min: 1, max: 2, firstFlexible: 1 },
    ADD_RAFT_VOTER: { apiKey: 80, min: 0, max: 1, firstFlexible: 0 },
    REMOVE_RAFT_VOTER: { apiKey: 81, min: 0, max: 0, firstFlexible: 0 },
    INITIALIZE_SHARE_GROUP_STATE: { apiKey: 83, min: 0, max: 0, firstFlexible: 0 },
    READ_SHARE_GROUP_STATE: { apiKey: 84, min: 0, max: 0, firstFlexible: 0 },
    WRITE_SHARE_GROUP_STATE: { apiKey: 85, min: 0, max: 1, firstFlexible: 0 },
    DELETE_SHARE_GROUP_STATE: { apiKey: 86, min: 0, max: 0, firstFlexible: 0 },
    READ_SHARE_GROUP_STATE_SUMMARY: { apiKey: 87, min: 0, max: 1, firstFlexible: 0 },
    STREAMS_GROUP_HEARTBEAT: { apiKey: 88, min: 0, max: 0, firstFlexible: 0 },
    STREAMS_GROUP_DESCRIBE: { apiKey: 89, min: 0, max: 0, firstFlexible: 0 },
    DESCRIBE_SHARE_GROUP_OFFSETS: { apiKey: 90, min: 0, max: 1, firstFlexible: 0 },
    ALTER_SHARE_GROUP_OFFSETS: { apiKey: 91, min: 0, max: 0, firstFlexible: 0 },
    DELETE_SHARE_GROUP_OFFSETS: { apiKey: 92, min: 0, max: 0, firstFlexible: 0 },
};

describe.each(Object.entries(SPEC) as [keyof typeof API, (typeof SPEC)[keyof typeof API]][])(
    '%s',
    (name, { apiKey, min, max, firstFlexible }) => {
        const versions = getVersions(API[name] as Api<unknown, unknown>);

        it('falls back through every version listed in the protocol spec', () => {
            expect(versions.map(({ apiVersion }) => apiVersion)).toEqual(
                Array.from({ length: max - min + 1 }, (_, index) => min + index),
            );
            versions.forEach((version) => expect(version.apiKey).toBe(apiKey));
        });

        it('uses flexible headers from the first flexible version', () => {
            versions.forEach(({ apiVersion, requestHeaderVersion, responseHeaderVersion }) => {
                const isFlexible = firstFlexible !== null && apiVersion >= firstFlexible;
                expect(requestHeaderVersion, `request header of v${apiVersion}`).toBe(isFlexible ? 2 : 1);
                expect(responseHeaderVersion, `response header of v${apiVersion}`).toBe(
                    isFlexible && name !== 'API_VERSIONS' ? 1 : 0,
                );
            });
        });
    },
);

describe('ApiVersions', () => {
    const unsupportedVersionResponse = () =>
        new Decoder(
            new Encoder()
                .writeInt16(API_ERROR.UNSUPPORTED_VERSION)
                .writeArray([{ apiKey: 18, minVersion: 0, maxVersion: 2 }], (encoder, version) =>
                    encoder.writeInt16(version.apiKey).writeInt16(version.minVersion).writeInt16(version.maxVersion),
                )
                .value(),
        );

    it.each(getVersions(API.API_VERSIONS as Api<unknown, unknown>))(
        'v$apiVersion decodes the version 0 response the broker sends for an unsupported version',
        (api) => {
            const decoder = unsupportedVersionResponse();

            expect(() => api.response(decoder)).toThrow(
                expect.objectContaining({
                    errorCode: API_ERROR.UNSUPPORTED_VERSION,
                    response: expect.objectContaining({
                        versions: [{ apiKey: 18, minVersion: 0, maxVersion: 2, tags: {} }],
                    }),
                }) as KafkaTSApiError,
            );
            expect(decoder.getOffset()).toBe(decoder.getBufferLength());
        },
    );
});
