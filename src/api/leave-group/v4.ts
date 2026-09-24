import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LEAVE_GROUP_V3 } from './v3';

/*
LeaveGroup Request (Version: 4) => { group_id (members) }
  group_id => COMPACT_STRING
  members => { member_id group_instance_id }
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING

LeaveGroup Response (Version: 4) => { throttle_time_ms error_code (members) }
  throttle_time_ms => INT32
  error_code => INT16
  members => { member_id group_instance_id error_code }
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    error_code => INT16
*/
export const LEAVE_GROUP_V4 = createApi({
    ...LEAVE_GROUP_V3,
    apiVersion: 4,
    fallback: LEAVE_GROUP_V3,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactArray(data.members, (encoder, member) =>
                encoder.writeCompactString(member.memberId).writeCompactString(member.groupInstanceId).writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            members: decoder.readCompactArray((member) => ({
                memberId: member.readCompactString()!,
                groupInstanceId: member.readCompactString(),
                errorCode: member.readInt16(),
                tags: member.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
