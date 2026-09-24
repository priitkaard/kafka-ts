import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { HEARTBEAT_V0 } from './v0';

/*
Heartbeat Request (Version: 1) => { group_id generation_id member_id }
  group_id => STRING
  generation_id => INT32
  member_id => STRING

Heartbeat Response (Version: 1) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const HEARTBEAT_V1 = createApi({
    ...HEARTBEAT_V0,
    apiVersion: 1,
    fallback: HEARTBEAT_V0,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            tags: {},
        }),
});
