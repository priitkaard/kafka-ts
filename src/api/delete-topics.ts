import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type DeleteTopicsRequest = {
    topics: {
        name: string;
        topicId: string | null;
    }[];
    timeoutMs?: number;
};

type DeleteTopicsResponse = {
    throttleTimeMs: number;
    responses: {
        name: string | null;
        _topicId: string;
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

/*
DeleteTopics Request (Version: 1) => [topic_names] timeout_ms 
  topic_names => STRING
  timeout_ms => INT32

DeleteTopics Response (Version: 1) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => name error_code 
    name => STRING
    error_code => INT16
*/
const DELETE_TOPICS_V1 = createApi<DeleteTopicsRequest, DeleteTopicsResponse>({
    apiKey: 20,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.topics, (encoder, topic) => encoder.writeString(topic.name))
            .writeInt32(data.timeoutMs ?? 10_000),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readArray((decoder) => ({
                name: decoder.readString()!,
                _topicId: '', // TopicId not present in v1 response
                errorCode: decoder.readInt16(),
                errorMessage: null,
                tags: {},
            })),
            tags: {},
        };
        result.responses.forEach((response) => {
            if (response.errorCode) throw new KafkaTSApiError(response.errorCode, response.errorMessage, result);
        });
        return result;
    },
});

/*
DeleteTopics Request (Version: 6) => [topics] timeout_ms _tagged_fields 
  topics => name topic_id _tagged_fields 
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
  timeout_ms => INT32

DeleteTopics Response (Version: 6) => throttle_time_ms [responses] _tagged_fields 
  throttle_time_ms => INT32
  responses => name topic_id error_code error_message _tagged_fields 
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const DELETE_TOPICS = createApi<DeleteTopicsRequest, DeleteTopicsResponse>({
    apiKey: 20,
    apiVersion: 6,
    fallback: DELETE_TOPICS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
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
                _topicId: decoder.readUUID(),
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
