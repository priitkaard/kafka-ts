import { KafkaTSApiError } from '../../utils/error';

export type PushTelemetryRequest = {
    clientInstanceId: string | null;
    subscriptionId: number;
    terminating: boolean;
    compressionType: number;
    metrics: Buffer;
};

export type PushTelemetryResponse = {
    throttleTimeMs: number;
    errorCode: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends PushTelemetryResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
