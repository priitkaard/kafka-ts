import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const LEAVE_GROUP = createApi({
    apiKey: 13,
    apiVersion: 5,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
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
            .writeCompactString(body.groupId)
            .writeCompactArray(body.members, (encoder, member) =>
                encoder
                    .writeCompactString(member.memberId)
                    .writeCompactString(member.groupInstanceId)
                    .writeCompactString(member.reason)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            members: decoder.readCompactArray((decoder) => ({
                memberId: decoder.readCompactString()!,
                groupInstanceId: decoder.readCompactString(),
                errorCode: decoder.readInt16(),
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        result.members.forEach((member) => {
            if (member.errorCode) throw new KafkaTSApiError(member.errorCode, null, result);
        });
        return result;
    },
});
