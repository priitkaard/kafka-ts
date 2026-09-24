import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { HEARTBEAT_V3 } from './v3';

/*
Heartbeat Request (Version: 4) => { group_id generation_id member_id group_instance_id }
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING

Heartbeat Response (Version: 4) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const HEARTBEAT_V4 = createApi({
    ...HEARTBEAT_V3,
    apiVersion: 4,
    fallback: HEARTBEAT_V3,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeInt32(data.generationId)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            tags: decoder.readTagBuffer(),
        }),
});
