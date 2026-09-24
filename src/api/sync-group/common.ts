import { Decoder } from '../../utils/decoder';
import { Encoder } from '../../utils/encoder';
import { KafkaTSApiError } from '../../utils/error';

export type Assignment = { [topic: string]: number[] };

export type MemberAssignment = {
    memberId: string;
    assignment: Assignment;
};

export type SyncGroupRequest = {
    groupId: string;
    generationId: number;
    memberId: string;
    groupInstanceId: string | null;
    protocolType: string | null;
    protocolName: string | null;
    assignments: MemberAssignment[];
};

export type SyncGroupResponse = {
    throttleTimeMs: number;
    errorCode: number;
    protocolType: string | null;
    protocolName: string | null;
    assignment: Assignment;
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: SyncGroupResponse) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};

export const encodeAssignment = (data: Assignment) =>
    new Encoder()
        .writeInt16(0)
        .writeArray(Object.entries(data), (encoder, [topic, partitions]) =>
            encoder.writeString(topic).writeArray(partitions, (encoder, partition) => encoder.writeInt32(partition)),
        )
        .writeBytes(Buffer.alloc(0))
        .value();

export const decodeAssignment = (data: Buffer | null): Assignment => {
    if (!data) {
        return {};
    }

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
