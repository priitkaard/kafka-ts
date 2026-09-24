import { createApi } from '../../utils/api';
import { decodeAssignment, encodeAssignment, throwIfError } from './common';
import { SYNC_GROUP_V3 } from './v3';

/*
SyncGroup Request (Version: 4) => { group_id generation_id member_id group_instance_id (assignments) }
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  assignments => { member_id assignment }
    member_id => COMPACT_STRING
    assignment => COMPACT_BYTES

SyncGroup Response (Version: 4) => { throttle_time_ms error_code assignment }
  throttle_time_ms => INT32
  error_code => INT16
  assignment => COMPACT_BYTES
*/
export const SYNC_GROUP_V4 = createApi({
    ...SYNC_GROUP_V3,
    apiVersion: 4,
    fallback: SYNC_GROUP_V3,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeInt32(data.generationId)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeCompactArray(data.assignments, (encoder, assignment) =>
                encoder
                    .writeCompactString(assignment.memberId)
                    .writeCompactBytes(encodeAssignment(assignment.assignment))
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            protocolType: null,
            protocolName: null,
            assignment: decodeAssignment(decoder.readCompactBytes()),
            tags: decoder.readTagBuffer(),
        }),
});
