import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { JOIN_GROUP_V6 } from './v6';

/*
JoinGroup Request (Version: 7) => { group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type (protocols) }
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => { name metadata }
    name => COMPACT_STRING
    metadata => COMPACT_BYTES

JoinGroup Response (Version: 7) => { throttle_time_ms error_code generation_id protocol_type protocol_name leader member_id (members) }
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  leader => COMPACT_STRING
  member_id => COMPACT_STRING
  members => { member_id group_instance_id metadata }
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
*/
export const JOIN_GROUP_V7 = createApi({
    ...JOIN_GROUP_V6,
    apiVersion: 7,
    fallback: JOIN_GROUP_V6,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            generationId: decoder.readInt32(),
            protocolType: decoder.readCompactString(),
            protocolName: decoder.readCompactString(),
            leader: decoder.readCompactString()!,
            skipAssignment: false,
            memberId: decoder.readCompactString()!,
            members: decoder.readCompactArray((member) => ({
                memberId: member.readCompactString()!,
                groupInstanceId: member.readCompactString(),
                metadata: member.readCompactBytes()!,
                tags: member.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
