import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DELETE_TOPICS_V5 } from './v5';

/*
DeleteTopics Request (Version: 6) => { (topics) timeout_ms }
  topics => { name topic_id }
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
  timeout_ms => INT32

DeleteTopics Response (Version: 6) => { throttle_time_ms (responses) }
  throttle_time_ms => INT32
  responses => { name topic_id error_code error_message }
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const DELETE_TOPICS_V6 = createApi({
    ...DELETE_TOPICS_V5,
    apiVersion: 6,
    fallback: DELETE_TOPICS_V5,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder.writeCompactString(topic.name).writeUUID(topic.topicId).writeTagBuffer(),
            )
            .writeInt32(data.timeoutMs ?? 10_000)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readCompactArray((response) => ({
                name: response.readCompactString(),
                _topicId: response.readUUID(),
                errorCode: response.readInt16(),
                errorMessage: response.readCompactString(),
                tags: response.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
