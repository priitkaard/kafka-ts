import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const HEARTBEAT = createApi({
    apiKey: 12,
    apiVersion: 4,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
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
            .writeCompactString(data.groupId)
            .writeInt32(data.generationId)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
