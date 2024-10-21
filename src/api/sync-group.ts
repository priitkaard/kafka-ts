import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export type Assignment = { [topic: string]: number[] };

export type MemberAssignment = {
    memberId: string;
    assignment: Assignment;
};

export const SYNC_GROUP = createApi({
    apiKey: 14,
    apiVersion: 5,
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
            .writeUVarInt(0)
            .writeCompactString(data.groupId)
            .writeInt32(data.generationId)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeCompactString(data.protocolType)
            .writeCompactString(data.protocolName)
            .writeCompactArray(data.assignments, (encoder, assignment) =>
                encoder
                    .writeCompactString(assignment.memberId)
                    .writeCompactString(JSON.stringify(assignment.assignment))
                    .writeUVarInt(0),
            )
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            protocolType: decoder.readCompactString(),
            protocolName: decoder.readCompactString(),
            assignments: decoder.readCompactString()!,
            _tag2: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
