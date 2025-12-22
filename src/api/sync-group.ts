import { createApi } from '../utils/api';
import { Decoder } from '../utils/decoder';
import { Encoder } from '../utils/encoder';
import { KafkaTSApiError } from '../utils/error';

export type Assignment = { [topic: string]: number[] };

export type MemberAssignment = {
    memberId: string;
    assignment: Assignment;
};

export const SYNC_GROUP = createApi({
    apiKey: 14,
    apiVersion: 5,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (
        encoder,
        data: {
            groupId: string;
            generationId: number;
            memberId: string;
            groupInstanceId: string | null;
            protocolType: string | null;
            protocolName: string | null;
            assignments: MemberAssignment[];
        },
    ) =>
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
