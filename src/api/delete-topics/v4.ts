import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DELETE_TOPICS_V3 } from './v3';

/*
DeleteTopics Request (Version: 4) => { (topic_names) timeout_ms }
  topic_names => COMPACT_STRING
  timeout_ms => INT32

DeleteTopics Response (Version: 4) => { throttle_time_ms (responses) }
  throttle_time_ms => INT32
  responses => { name error_code }
    name => COMPACT_STRING
    error_code => INT16
*/
export const DELETE_TOPICS_V4 = createApi({
    ...DELETE_TOPICS_V3,
    apiVersion: 4,
    fallback: DELETE_TOPICS_V3,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) => encoder.writeCompactString(topic.name))
            .writeInt32(data.timeoutMs ?? 10_000)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readCompactArray((response) => ({
                name: response.readCompactString()!,
                _topicId: '',
                errorCode: response.readInt16(),
                errorMessage: null,
                tags: response.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
