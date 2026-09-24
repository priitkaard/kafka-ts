import { createApi } from '../../utils/api';
import { encodeProtocolMetadata } from './common';
import { JOIN_GROUP_V7 } from './v7';

/*
JoinGroup Request (Version: 8) => { group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type (protocols) reason }
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => { name metadata }
    name => COMPACT_STRING
    metadata => COMPACT_BYTES
  reason => COMPACT_NULLABLE_STRING

JoinGroup Response (Version: 8) => { throttle_time_ms error_code generation_id protocol_type protocol_name leader member_id (members) }
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
export const JOIN_GROUP_V8 = createApi({
    ...JOIN_GROUP_V7,
    apiVersion: 8,
    fallback: JOIN_GROUP_V7,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeInt32(data.sessionTimeoutMs)
            .writeInt32(data.rebalanceTimeoutMs)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeCompactString(data.protocolType)
            .writeCompactArray(data.protocols, (encoder, protocol) =>
                encoder
                    .writeCompactString(protocol.name)
                    .writeCompactBytes(encodeProtocolMetadata(protocol.metadata))
                    .writeTagBuffer(),
            )
            .writeCompactString(data.reason)
            .writeTagBuffer(),
});
