import { createApi } from '../../utils/api';
import { decodeAssignment, encodeAssignment, throwIfError } from './common';
import { SYNC_GROUP_V4 } from './v4';

/*
SyncGroup Request (Version: 5) => { group_id generation_id member_id group_instance_id protocol_type protocol_name (assignments) }
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  assignments => { member_id assignment }
    member_id => COMPACT_STRING
    assignment => COMPACT_BYTES

SyncGroup Response (Version: 5) => { throttle_time_ms error_code protocol_type protocol_name assignment }
  throttle_time_ms => INT32
  error_code => INT16
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  assignment => COMPACT_BYTES
*/
export const SYNC_GROUP_V5 = createApi({
    ...SYNC_GROUP_V4,
    apiVersion: 5,
    fallback: SYNC_GROUP_V4,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeInt32(data.generationId)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeCompactString(data.protocolType)
            .writeCompactString(data.protocolName)
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
            protocolType: decoder.readCompactString(),
            protocolName: decoder.readCompactString(),
            assignment: decodeAssignment(decoder.readCompactBytes()),
            tags: decoder.readTagBuffer(),
        }),
});
