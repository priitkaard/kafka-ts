import { createApi } from '../../utils/api';
import { decodeAssignment, throwIfError } from './common';
import { SYNC_GROUP_V0 } from './v0';

/*
SyncGroup Request (Version: 1) => { group_id generation_id member_id [assignments] }
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  assignments => { member_id assignment }
    member_id => STRING
    assignment => BYTES

SyncGroup Response (Version: 1) => { throttle_time_ms error_code assignment }
  throttle_time_ms => INT32
  error_code => INT16
  assignment => BYTES
*/
export const SYNC_GROUP_V1 = createApi({
    ...SYNC_GROUP_V0,
    apiVersion: 1,
    fallback: SYNC_GROUP_V0,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            protocolType: null,
            protocolName: null,
            assignment: decodeAssignment(decoder.readBytes()),
            tags: {},
        }),
});
