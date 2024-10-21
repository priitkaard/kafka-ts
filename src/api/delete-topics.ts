import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const DELETE_TOPICS = createApi({
    apiKey: 20,
    apiVersion: 6,
    request: (
        encoder,
        data: {
            topics: {
                name: string | null;
                topicId: string | null;
            }[];
            timeoutMs: number;
        },
    ) =>
        encoder
            .writeUVarInt(0)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder.writeCompactString(topic.name).writeUUID(topic.topicId).writeUVarInt(0),
            )
            .writeInt32(data.timeoutMs)
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readCompactArray((decoder) => ({
                name: decoder.readCompactString(),
                topicId: decoder.readUUID(),
                errorCode: decoder.readInt16(),
                errorMessage: decoder.readCompactString(),
                _tag: decoder.readTagBuffer(),
            })),
            _tag2: decoder.readTagBuffer(),
        };
        result.responses.forEach((response) => {
            if (response.errorCode) throw new KafkaTSApiError(response.errorCode, response.errorMessage, result);
        });
        return result;
    },
});
