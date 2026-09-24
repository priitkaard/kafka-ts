import { createApi } from '../../utils/api';
import { HEARTBEAT_V2 } from './v2';

/*
Heartbeat Request (Version: 3) => { group_id generation_id member_id group_instance_id }
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING

Heartbeat Response (Version: 3) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const HEARTBEAT_V3 = createApi({
    ...HEARTBEAT_V2,
    apiVersion: 3,
    fallback: HEARTBEAT_V2,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.generationId)
            .writeString(data.memberId)
            .writeString(data.groupInstanceId),
});
