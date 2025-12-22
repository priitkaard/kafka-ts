import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type HeartbeatRequest = {
    groupId: string;
    generationId: number;
    memberId: string;
    groupInstanceId: string | null;
};

type HeartbeatResponse = {
    throttleTimeMs: number;
    errorCode: number;
    tags: Record<number, Buffer>;
};

/*
Heartbeat Request (Version: 0) => group_id generation_id member_id 
  group_id => STRING
  generation_id => INT32
  member_id => STRING

Heartbeat Response (Version: 0) => error_code 
  error_code => INT16
*/
const HEARTBEAT_V0 = createApi<HeartbeatRequest, HeartbeatResponse>({
    apiKey: 12,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeString(data.groupId).writeInt32(data.generationId).writeString(data.memberId),
    response: (decoder) => {
        const result = {
            throttleTimeMs: 0,
            errorCode: decoder.readInt16(),
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});

/*
Heartbeat Request (Version: 4) => group_id generation_id member_id group_instance_id _tagged_fields 
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING

Heartbeat Response (Version: 4) => throttle_time_ms error_code _tagged_fields 
  throttle_time_ms => INT32
  error_code => INT16
*/
export const HEARTBEAT = createApi<HeartbeatRequest, HeartbeatResponse>({
    apiKey: 12,
    apiVersion: 4,
    fallback: HEARTBEAT_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeInt32(data.generationId)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
