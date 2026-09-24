import { createApi } from '../../utils/api';
import { encodeProtocolMetadata } from './common';
import { JOIN_GROUP_V0 } from './v0';

/*
JoinGroup Request (Version: 1) => { group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols] }
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  protocol_type => STRING
  protocols => { name metadata }
    name => STRING
    metadata => BYTES

JoinGroup Response (Version: 1) => { error_code generation_id protocol_name leader member_id [members] }
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => { member_id metadata }
    member_id => STRING
    metadata => BYTES
*/
export const JOIN_GROUP_V1 = createApi({
    ...JOIN_GROUP_V0,
    apiVersion: 1,
    fallback: JOIN_GROUP_V0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.sessionTimeoutMs)
            .writeInt32(data.rebalanceTimeoutMs)
            .writeString(data.memberId)
            .writeString(data.protocolType)
            .writeArray(data.protocols, (encoder, protocol) =>
                encoder.writeString(protocol.name).writeBytes(encodeProtocolMetadata(protocol.metadata)),
            ),
});
