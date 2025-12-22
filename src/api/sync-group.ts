import { createApi } from '../utils/api';
import { Decoder } from '../utils/decoder';
import { Encoder } from '../utils/encoder';
import { KafkaTSApiError } from '../utils/error';

export type Assignment = { [topic: string]: number[] };

export type MemberAssignment = {
    memberId: string;
    assignment: Assignment;
};

type SyncGroupRequest = {
    groupId: string;
    generationId: number;
    memberId: string;
    groupInstanceId: string | null;
    protocolType: string | null;
    protocolName: string | null;
    assignments: MemberAssignment[];
};

type SyncGroupResponse = {
    throttleTimeMs: number;
    errorCode: number;
    protocolType: string | null;
    protocolName: string | null;
    assignments: Assignment;
    tags: Record<number, Buffer>;
};

/*
SyncGroup Request (Version: 0) => group_id generation_id member_id [assignments] 
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  assignments => member_id assignment 
    member_id => STRING
    assignment => BYTES

SyncGroup Response (Version: 0) => error_code assignment 
  error_code => INT16
  assignment => BYTES
*/
const SYNC_GROUP_V0 = createApi<SyncGroupRequest, SyncGroupResponse>({
    apiKey: 14,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.generationId)
            .writeString(data.memberId)
            .writeArray(data.assignments, (encoder, assignment) =>
                encoder.writeString(assignment.memberId).writeBytes(encodeAssignment(assignment.assignment)),
            ),
    response: (decoder) => {
        const result = {
            throttleTimeMs: 0,
            errorCode: decoder.readInt16(),
            protocolType: null,
            protocolName: null,
            assignments: decodeAssignment(decoder.readBytes()!),
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});

/*
SyncGroup Request (Version: 5) => group_id generation_id member_id group_instance_id protocol_type protocol_name [assignments] _tagged_fields 
  group_id => COMPACT_STRING
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  assignments => member_id assignment _tagged_fields 
    member_id => COMPACT_STRING
    assignment => COMPACT_BYTES

SyncGroup Response (Version: 5) => throttle_time_ms error_code protocol_type protocol_name assignment _tagged_fields 
  throttle_time_ms => INT32
  error_code => INT16
  protocol_type => COMPACT_NULLABLE_STRING
  protocol_name => COMPACT_NULLABLE_STRING
  assignment => COMPACT_BYTES
*/
export const SYNC_GROUP = createApi<SyncGroupRequest, SyncGroupResponse>({
    apiKey: 14,
    apiVersion: 5,
    fallback: SYNC_GROUP_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeInt32(data.generationId)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeCompactString(data.protocolType)
            .writeCompactString(data.protocolName)
            .writeCompactArray(data.assignments, (encoder, assignment) =>
                encoder
                    .writeCompactString(assignment.memberId)
                    .writeCompactBytes(encodeAssignment(assignment.assignment))
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            protocolType: decoder.readCompactString(),
            protocolName: decoder.readCompactString(),
            assignments: decodeAssignment(decoder.readCompactBytes()!),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});

const encodeAssignment = (data: Assignment) =>
    new Encoder()
        .writeInt16(0)
        .writeArray(Object.entries(data), (encoder, [topic, partitions]) =>
            encoder.writeString(topic).writeArray(partitions, (encoder, partition) => encoder.writeInt32(partition)),
        )
        .writeBytes(Buffer.alloc(0))
        .value();

const decodeAssignment = (data: Buffer): Assignment => {
    const decoder = new Decoder(data);
    if (!decoder.getBufferLength()) {
        return {};
    }

    const result = {
        version: decoder.readInt16(),
        assignment: decoder.readArray((decoder) => ({
            topic: decoder.readString(),
            partitions: decoder.readArray((decoder) => decoder.readInt32()),
        })),
        userData: decoder.readBytes(),
    };
    return Object.fromEntries(result.assignment.map(({ topic, partitions }) => [topic, partitions]));
};
