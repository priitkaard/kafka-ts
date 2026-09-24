import { createApi } from '../../utils/api';
import { encodeProtocolMetadata, throwIfError } from './common';
import { JOIN_GROUP_V4 } from './v4';

/*
JoinGroup Request (Version: 5) => { group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] }
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING
  protocol_type => STRING
  protocols => { name metadata }
    name => STRING
    metadata => BYTES

JoinGroup Response (Version: 5) => { throttle_time_ms error_code generation_id protocol_name leader member_id [members] }
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => { member_id group_instance_id metadata }
    member_id => STRING
    group_instance_id => NULLABLE_STRING
    metadata => BYTES
*/
export const JOIN_GROUP_V5 = createApi({
    ...JOIN_GROUP_V4,
    apiVersion: 5,
    fallback: JOIN_GROUP_V4,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.sessionTimeoutMs)
            .writeInt32(data.rebalanceTimeoutMs)
            .writeString(data.memberId)
            .writeString(data.groupInstanceId)
            .writeString(data.protocolType)
            .writeArray(data.protocols, (encoder, protocol) =>
                encoder.writeString(protocol.name).writeBytes(encodeProtocolMetadata(protocol.metadata)),
            ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            generationId: decoder.readInt32(),
            protocolType: null,
            protocolName: decoder.readString(),
            leader: decoder.readString()!,
            skipAssignment: false,
            memberId: decoder.readString()!,
            members: decoder.readArray((member) => ({
                memberId: member.readString()!,
                groupInstanceId: member.readString(),
                metadata: member.readBytes()!,
                tags: {},
            })),
            tags: {},
        }),
});
