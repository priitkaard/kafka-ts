import { createApi } from '../../utils/api';
import { LEAVE_GROUP_V4 } from './v4';

/*
LeaveGroup Request (Version: 5) => { group_id (members) }
  group_id => COMPACT_STRING
  members => { member_id group_instance_id reason }
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    reason => COMPACT_NULLABLE_STRING

LeaveGroup Response (Version: 5) => { throttle_time_ms error_code (members) }
  throttle_time_ms => INT32
  error_code => INT16
  members => { member_id group_instance_id error_code }
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    error_code => INT16
*/
export const LEAVE_GROUP_V5 = createApi({
    ...LEAVE_GROUP_V4,
    apiVersion: 5,
    fallback: LEAVE_GROUP_V4,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactArray(data.members, (encoder, member) =>
                encoder
                    .writeCompactString(member.memberId)
                    .writeCompactString(member.groupInstanceId)
                    .writeCompactString(member.reason)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
});
