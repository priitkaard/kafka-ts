import { createApi } from '../../utils/api';
import { encodeAssignment } from './common';
import { SYNC_GROUP_V2 } from './v2';

/*
SyncGroup Request (Version: 3) => { group_id generation_id member_id group_instance_id [assignments] }
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING
  assignments => { member_id assignment }
    member_id => STRING
    assignment => BYTES

SyncGroup Response (Version: 3) => { throttle_time_ms error_code assignment }
  throttle_time_ms => INT32
  error_code => INT16
  assignment => BYTES
*/
export const SYNC_GROUP_V3 = createApi({
    ...SYNC_GROUP_V2,
    apiVersion: 3,
    fallback: SYNC_GROUP_V2,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.generationId)
            .writeString(data.memberId)
            .writeString(data.groupInstanceId)
            .writeArray(data.assignments, (encoder, assignment) =>
                encoder.writeString(assignment.memberId).writeBytes(encodeAssignment(assignment.assignment)),
            ),
});
