import { createApi } from '../../utils/api';
import { DeleteTopicsRequest, DeleteTopicsResponse, throwIfError } from './common';

/*
DeleteTopics Request (Version: 1) => { [topic_names] timeout_ms }
  topic_names => STRING
  timeout_ms => INT32

DeleteTopics Response (Version: 1) => { throttle_time_ms [responses] }
  throttle_time_ms => INT32
  responses => { name error_code }
    name => STRING
    error_code => INT16
*/
export const DELETE_TOPICS_V1 = createApi<DeleteTopicsRequest, DeleteTopicsResponse>({
    apiKey: 20,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.topics, (encoder, topic) => encoder.writeString(topic.name))
            .writeInt32(data.timeoutMs ?? 10_000),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readArray((response) => ({
                name: response.readString()!,
                _topicId: '',
                errorCode: response.readInt16(),
                errorMessage: null,
                tags: {},
            })),
            tags: {},
        }),
});
