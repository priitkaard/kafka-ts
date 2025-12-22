import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const DELETE_TOPICS = createApi({
    apiKey: 20,
    apiVersion: 6,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (
        encoder,
        data: {
            topics: {
                name: string | null;
                topicId: string | null;
            }[];
            timeoutMs?: number;
        },
    ) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder.writeCompactString(topic.name).writeUUID(topic.topicId).writeTagBuffer(),
            )
            .writeInt32(data.timeoutMs ?? 10_000)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readCompactArray((decoder) => ({
                name: decoder.readCompactString(),
                topicId: decoder.readUUID(),
                errorCode: decoder.readInt16(),
                errorMessage: decoder.readCompactString(),
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        result.responses.forEach((response) => {
            if (response.errorCode) throw new KafkaTSApiError(response.errorCode, response.errorMessage, result);
        });
        return result;
    },
});
