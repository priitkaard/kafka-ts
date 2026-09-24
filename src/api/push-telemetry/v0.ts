import { createApi } from '../../utils/api';
import { PushTelemetryRequest, PushTelemetryResponse, throwIfError } from './common';

/*
PushTelemetry Request (Version: 0) => { client_instance_id subscription_id terminating compression_type metrics }
  client_instance_id => UUID
  subscription_id => INT32
  terminating => BOOLEAN
  compression_type => INT8
  metrics => COMPACT_BYTES

PushTelemetry Response (Version: 0) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const PUSH_TELEMETRY_V0 = createApi<PushTelemetryRequest, PushTelemetryResponse>({
    apiKey: 72,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeUUID(data.clientInstanceId)
            .writeInt32(data.subscriptionId)
            .writeBoolean(data.terminating)
            .writeInt8(data.compressionType)
            .writeCompactBytes(data.metrics)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            tags: decoder.readTagBuffer(),
        }),
});
