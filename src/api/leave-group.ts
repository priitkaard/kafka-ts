import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type LeaveGroupRequest = {
    groupId: string;
    members: {
        memberId: string;
        groupInstanceId: string | null;
        reason: string | null;
    }[];
};

type LeaveGroupResponse = {
    throttleTimeMs: number;
    errorCode: number;
    members: {
        memberId: string;
        groupInstanceId: string | null;
        errorCode: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

/*
LeaveGroup Request (Version: 0) => group_id member_id 
  group_id => STRING
  member_id => STRING

LeaveGroup Response (Version: 0) => error_code 
  error_code => INT16
*/

const LEAVE_GROUP_V0 = createApi<LeaveGroupRequest, LeaveGroupResponse>({
    apiKey: 13,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, body) => encoder.writeString(body.groupId).writeString(body.members[0].memberId),
    response: (decoder) => {
        const result = {
            throttleTimeMs: 0,
            errorCode: decoder.readInt16(),
            members: [],
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});

/*
LeaveGroup Request (Version: 5) => group_id [members] _tagged_fields 
  group_id => COMPACT_STRING
  members => member_id group_instance_id reason _tagged_fields 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    reason => COMPACT_NULLABLE_STRING

LeaveGroup Response (Version: 5) => throttle_time_ms error_code [members] _tagged_fields 
  throttle_time_ms => INT32
  error_code => INT16
  members => member_id group_instance_id error_code _tagged_fields 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    error_code => INT16
*/
export const LEAVE_GROUP = createApi<LeaveGroupRequest, LeaveGroupResponse>({
    apiKey: 13,
    apiVersion: 5,
    fallback: LEAVE_GROUP_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, body) =>
        encoder
            .writeCompactString(body.groupId)
            .writeCompactArray(body.members, (encoder, member) =>
                encoder
                    .writeCompactString(member.memberId)
                    .writeCompactString(member.groupInstanceId)
                    .writeCompactString(member.reason)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            members: decoder.readCompactArray((decoder) => ({
                memberId: decoder.readCompactString()!,
                groupInstanceId: decoder.readCompactString(),
                errorCode: decoder.readInt16(),
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        result.members.forEach((member) => {
            if (member.errorCode) throw new KafkaTSApiError(member.errorCode, null, result);
        });
        return result;
    },
});
