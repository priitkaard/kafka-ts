import { createApi } from '../../utils/api';
import { encodeProtocolMetadata, throwIfError } from './common';
import { JOIN_GROUP_V5 } from './v5';

/*
JoinGroup Request (Version: 6) => { group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type (protocols) }
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => { name metadata }
    name => COMPACT_STRING
    metadata => COMPACT_BYTES

JoinGroup Response (Version: 6) => { throttle_time_ms error_code generation_id protocol_name leader member_id (members) }
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => COMPACT_STRING
  leader => COMPACT_STRING
  member_id => COMPACT_STRING
  members => { member_id group_instance_id metadata }
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
*/
export const JOIN_GROUP_V6 = createApi({
    ...JOIN_GROUP_V5,
    apiVersion: 6,
    fallback: JOIN_GROUP_V5,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
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
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            generationId: decoder.readInt32(),
            protocolType: null,
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
