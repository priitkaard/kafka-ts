import { createApi } from '../utils/api';
import { Encoder } from '../utils/encoder';
import { KafkaTSApiError } from '../utils/error';

type JoinGroupRequest = {
    groupId: string;
    sessionTimeoutMs: number;
    rebalanceTimeoutMs: number;
    memberId: string;
    groupInstanceId: string | null;
    protocolType: string;
    protocols: {
        name: string;
        metadata: {
            version: number;
            topics: string[];
        };
    }[];
    reason: string | null;
};

type JoinGroupResponse = {
    throttleTimeMs: number;
    errorCode: number;
    generationId: number;
    protocolType: string | null;
    protocolName: string | null;
    leader: string;
    skipAssignment: boolean;
    memberId: string;
    members: {
        memberId: string;
        groupInstanceId: string | null;
        metadata: Buffer;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

/*
JoinGroup Request (Version: 5) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] 
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING
  protocol_type => STRING
  protocols => name metadata 
    name => STRING
    metadata => BYTES

JoinGroup Response (Version: 5) => throttle_time_ms error_code generation_id protocol_name leader member_id [members] 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => member_id group_instance_id metadata 
    member_id => STRING
    group_instance_id => NULLABLE_STRING
    metadata => BYTES
*/
const JOIN_GROUP_V5 = createApi<JoinGroupRequest, JoinGroupResponse>({
    apiKey: 11,
    apiVersion: 5,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.sessionTimeoutMs)
            .writeInt32(data.rebalanceTimeoutMs)
            .writeString(data.memberId)
            .writeString(data.groupInstanceId)
            .writeString(data.protocolType)
            .writeArray(data.protocols, (encoder, protocol) => {
                const metadata = new Encoder()
                    .writeInt16(protocol.metadata.version)
                    .writeArray(protocol.metadata.topics, (encoder, topic) => encoder.writeString(topic))
                    .writeBytes(Buffer.alloc(0))
                    .value();
                return encoder.writeString(protocol.name).writeBytes(metadata);
            }),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            generationId: decoder.readInt32(),
            protocolType: null,
            protocolName: decoder.readString()!,
            leader: decoder.readString()!,
            skipAssignment: false,
            memberId: decoder.readString()!,
            members: decoder.readArray((decoder) => ({
                memberId: decoder.readString()!,
                groupInstanceId: decoder.readString(),
                metadata: decoder.readBytes()!,
                tags: {},
            })),
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});

/*
JoinGroup Request (Version: 6) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] _tagged_fields 
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => name metadata _tagged_fields 
    name => COMPACT_STRING
    metadata => COMPACT_BYTES

JoinGroup Response (Version: 6) => throttle_time_ms error_code generation_id protocol_name leader member_id [members] _tagged_fields 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => COMPACT_STRING
  leader => COMPACT_STRING
  member_id => COMPACT_STRING
  members => member_id group_instance_id metadata _tagged_fields 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
*/
const JOIN_GROUP_V6 = createApi<JoinGroupRequest, JoinGroupResponse>({
    apiKey: 11,
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
            .writeCompactArray(data.protocols, (encoder, protocol) => {
                const metadata = new Encoder()
                    .writeInt16(protocol.metadata.version)
                    .writeArray(protocol.metadata.topics, (encoder, topic) => encoder.writeString(topic))
                    .writeBytes(Buffer.alloc(0))
                    .value();
                return encoder.writeCompactString(protocol.name).writeCompactBytes(metadata).writeTagBuffer();
            })
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            generationId: decoder.readInt32(),
            protocolType: null,
            protocolName: decoder.readCompactString()!,
            leader: decoder.readCompactString()!,
            skipAssignment: false,
            memberId: decoder.readCompactString()!,
            members: decoder.readCompactArray((decoder) => ({
                memberId: decoder.readCompactString()!,
                groupInstanceId: decoder.readCompactString(),
                metadata: decoder.readCompactBytes()!,
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});

/*
JoinGroup Request (Version: 9) => group_id session_timeout_ms rebalance_timeout_ms member_id group_instance_id protocol_type [protocols] reason _tagged_fields 
  group_id => COMPACT_STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_STRING
  protocols => name metadata _tagged_fields 
    name => COMPACT_STRING
    metadata => COMPACT_BYTES
  reason => COMPACT_NULLABLE_STRING

JoinGroup Response (Version: 9) => throttle_time_ms error_code generation_id protocol_type protocol_name leader skip_assignment member_id [members] _tagged_fields 
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  leader => COMPACT_STRING
  skip_assignment => BOOLEAN
  member_id => COMPACT_STRING
  members => member_id group_instance_id metadata _tagged_fields 
    member_id => COMPACT_STRING
    group_instance_id => COMPACT_NULLABLE_STRING
    metadata => COMPACT_BYTES
*/
export const JOIN_GROUP = createApi<JoinGroupRequest, JoinGroupResponse>({
    apiKey: 11,
    apiVersion: 9,
    fallback: JOIN_GROUP_V6,
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
            .writeCompactArray(data.protocols, (encoder, protocol) => {
                const metadata = new Encoder()
                    .writeInt16(protocol.metadata.version)
                    .writeArray(protocol.metadata.topics, (encoder, topic) => encoder.writeString(topic))
                    .writeBytes(Buffer.alloc(0))
                    .value();
                return encoder.writeCompactString(protocol.name).writeCompactBytes(metadata).writeTagBuffer();
            })
            .writeCompactString(data.reason)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            generationId: decoder.readInt32(),
            protocolType: decoder.readCompactString(),
            protocolName: decoder.readCompactString(),
            leader: decoder.readCompactString()!,
            skipAssignment: decoder.readBoolean(),
            memberId: decoder.readCompactString()!,
            members: decoder.readCompactArray((decoder) => ({
                memberId: decoder.readCompactString()!,
                groupInstanceId: decoder.readCompactString(),
                metadata: decoder.readCompactBytes()!,
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
