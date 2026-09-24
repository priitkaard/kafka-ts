import { createApi } from '../../utils/api';
import { GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse, throwIfError } from './common';

/*
GetTelemetrySubscriptions Request (Version: 0) => { client_instance_id }
  client_instance_id => UUID

GetTelemetrySubscriptions Response (Version: 0) => { throttle_time_ms error_code client_instance_id subscription_id (accepted_compression_types) push_interval_ms telemetry_max_bytes delta_temporality (requested_metrics) }
  throttle_time_ms => INT32
  error_code => INT16
  client_instance_id => UUID
  subscription_id => INT32
  accepted_compression_types => INT8
  push_interval_ms => INT32
  telemetry_max_bytes => INT32
  delta_temporality => BOOLEAN
  requested_metrics => COMPACT_STRING
*/
export const GET_TELEMETRY_SUBSCRIPTIONS_V0 = createApi<
    GetTelemetrySubscriptionsRequest,
    GetTelemetrySubscriptionsResponse
>({
    apiKey: 71,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) => encoder.writeUUID(data.clientInstanceId).writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            clientInstanceId: decoder.readUUID(),
            subscriptionId: decoder.readInt32(),
            acceptedCompressionTypes: decoder.readCompactArray((acceptedCompressionType) =>
                acceptedCompressionType.readInt8(),
            ),
            pushIntervalMs: decoder.readInt32(),
            telemetryMaxBytes: decoder.readInt32(),
            deltaTemporality: decoder.readBoolean(),
            requestedMetrics: decoder.readCompactArray((requestedMetric) => requestedMetric.readCompactString()!),
            tags: decoder.readTagBuffer(),
        }),
});
