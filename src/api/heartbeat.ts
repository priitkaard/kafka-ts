import { createApi } from "../utils/api";
import { KafkaTSApiError } from "../utils/error";

export const HEARTBEAT = createApi({
    apiKey: 12,
    apiVersion: 4,
    request: (
        encoder,
        data: {
            groupId: string;
            generationId: number;
            memberId: string;
            groupInstanceId: string | null;
        },
    ) =>
        encoder
            .writeUVarInt(0)
            .writeCompactString(data.groupId)
            .writeInt32(data.generationId)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            _tag2: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
