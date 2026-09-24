import { createApi } from '../../utils/api';
import { encodeProtocolMetadata, JoinGroupRequest, JoinGroupResponse, throwIfError } from './common';

/*
JoinGroup Request (Version: 0) => { group_id session_timeout_ms member_id protocol_type [protocols] }
  group_id => STRING
  session_timeout_ms => INT32
  member_id => STRING
  protocol_type => STRING
  protocols => { name metadata }
    name => STRING
    metadata => BYTES

JoinGroup Response (Version: 0) => { error_code generation_id protocol_name leader member_id [members] }
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => { member_id metadata }
    member_id => STRING
    metadata => BYTES
*/
export const JOIN_GROUP_V0 = createApi<JoinGroupRequest, JoinGroupResponse>({
    apiKey: 11,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.sessionTimeoutMs)
            .writeString(data.memberId)
            .writeString(data.protocolType)
            .writeArray(data.protocols, (encoder, protocol) =>
                encoder.writeString(protocol.name).writeBytes(encodeProtocolMetadata(protocol.metadata)),
            ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
            errorCode: decoder.readInt16(),
            generationId: decoder.readInt32(),
            protocolType: null,
            protocolName: decoder.readString(),
            leader: decoder.readString()!,
            skipAssignment: false,
            memberId: decoder.readString()!,
            members: decoder.readArray((member) => ({
                memberId: member.readString()!,
                groupInstanceId: null,
                metadata: member.readBytes()!,
                tags: {},
            })),
            tags: {},
        }),
});
