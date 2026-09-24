export type ApiVersionsRequest = {
    clientSoftwareName?: string;
    clientSoftwareVersion?: string;
};

export type ApiVersionsResponse = {
    errorCode: number;
    versions: {
        apiKey: number;
        minVersion: number;
        maxVersion: number;
        tags: Record<number, Buffer>;
    }[];
    throttleTimeMs: number;
    tags: Record<number, Buffer>;
};

export const UNSUPPORTED_VERSION = 35;
