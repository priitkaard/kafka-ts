import { createApi } from '../../utils/api';
import { LeaveGroupRequest, LeaveGroupResponse, throwIfError } from './common';

/*
LeaveGroup Request (Version: 0) => { group_id member_id }
  group_id => STRING
  member_id => STRING

LeaveGroup Response (Version: 0) => { error_code }
  error_code => INT16
*/
export const LEAVE_GROUP_V0 = createApi<LeaveGroupRequest, LeaveGroupResponse>({
    apiKey: 13,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeString(data.groupId).writeString(data.members[0].memberId),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
            errorCode: decoder.readInt16(),
            members: [],
            tags: {},
        }),
});
