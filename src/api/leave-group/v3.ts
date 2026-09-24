import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LEAVE_GROUP_V2 } from './v2';

/*
LeaveGroup Request (Version: 3) => { group_id [members] }
  group_id => STRING
  members => { member_id group_instance_id }
    member_id => STRING
    group_instance_id => NULLABLE_STRING

LeaveGroup Response (Version: 3) => { throttle_time_ms error_code [members] }
  throttle_time_ms => INT32
  error_code => INT16
  members => { member_id group_instance_id error_code }
    member_id => STRING
    group_instance_id => NULLABLE_STRING
    error_code => INT16
*/
export const LEAVE_GROUP_V3 = createApi({
    ...LEAVE_GROUP_V2,
    apiVersion: 3,
    fallback: LEAVE_GROUP_V2,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeArray(data.members, (encoder, member) =>
                encoder.writeString(member.memberId).writeString(member.groupInstanceId),
            ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            members: decoder.readArray((member) => ({
                memberId: member.readString()!,
                groupInstanceId: member.readString(),
                errorCode: member.readInt16(),
                tags: {},
            })),
            tags: {},
        }),
});
