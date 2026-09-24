import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LEAVE_GROUP_V0 } from './v0';

/*
LeaveGroup Request (Version: 1) => { group_id member_id }
  group_id => STRING
  member_id => STRING

LeaveGroup Response (Version: 1) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const LEAVE_GROUP_V1 = createApi({
    ...LEAVE_GROUP_V0,
    apiVersion: 1,
    fallback: LEAVE_GROUP_V0,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            members: [],
            tags: {},
        }),
});
