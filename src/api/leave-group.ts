import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const LEAVE_GROUP = createApi({
    apiKey: 13,
    apiVersion: 5,
    request: (
        encoder,
        body: {
            groupId: string;
            members: {
                memberId: string;
                groupInstanceId: string | null;
                reason: string | null;
            }[];
        },
    ) =>
        encoder
            .writeUVarInt(0)
            .writeCompactString(body.groupId)
            .writeCompactArray(body.members, (encoder, member) =>
                encoder
                    .writeCompactString(member.memberId)
                    .writeCompactString(member.groupInstanceId)
                    .writeCompactString(member.reason)
                    .writeUVarInt(0),
            )
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            members: decoder.readCompactArray((decoder) => ({
                memberId: decoder.readCompactString()!,
                groupInstanceId: decoder.readCompactString(),
                errorCode: decoder.readInt16(),
                _tag: decoder.readTagBuffer(),
            })),
            _tag2: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        result.members.forEach((member) => {
            if (member.errorCode) throw new KafkaTSApiError(member.errorCode, null, result);
        });
        return result;
    },
});
