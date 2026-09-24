import { KafkaTSApiError } from '../../utils/error';

export type GetTelemetrySubscriptionsRequest = {
    clientInstanceId: string | null;
};

export type GetTelemetrySubscriptionsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    clientInstanceId: string;
    subscriptionId: number;
    acceptedCompressionTypes: number[];
    pushIntervalMs: number;
    telemetryMaxBytes: number;
    deltaTemporality: boolean;
    requestedMetrics: string[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends GetTelemetrySubscriptionsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
