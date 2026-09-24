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
    SASL_HANDSHAKE: { apiKey: 17, min: 0, max: 1, firstFlexible: null },
    API_VERSIONS: { apiKey: 18, min: 0, max: 4, firstFlexible: 3 },
    CREATE_TOPICS: { apiKey: 19, min: 2, max: 7, firstFlexible: 5 },
    DELETE_TOPICS: { apiKey: 20, min: 1, max: 6, firstFlexible: 4 },
    INIT_PRODUCER_ID: { apiKey: 22, min: 0, max: 6, firstFlexible: 2 },
    SASL_AUTHENTICATE: { apiKey: 36, min: 0, max: 2, firstFlexible: 2 },
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
