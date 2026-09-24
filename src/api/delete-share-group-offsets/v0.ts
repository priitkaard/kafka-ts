import { createApi } from '../../utils/api';
import { DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsResponse, throwIfError } from './common';

/*
DeleteShareGroupOffsets Request (Version: 0) => { group_id (topics) }
  group_id => COMPACT_STRING
  topics => { topic_name }
    topic_name => COMPACT_STRING

DeleteShareGroupOffsets Response (Version: 0) => { throttle_time_ms error_code error_message (responses) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => { topic_name topic_id error_code error_message }
    topic_name => COMPACT_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const DELETE_SHARE_GROUP_OFFSETS_V0 = createApi<DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsResponse>(
    {
        apiKey: 92,
        apiVersion: 0,
        requestHeaderVersion: 2,
        responseHeaderVersion: 1,
        request: (encoder, data) =>
            encoder
                .writeCompactString(data.groupId)
                .writeCompactArray(data.topics, (encoder, topic) =>
                    encoder.writeCompactString(topic.topicName).writeTagBuffer(),
                )
                .writeTagBuffer(),
        response: (decoder) =>
            throwIfError({
                throttleTimeMs: decoder.readInt32(),
                errorCode: decoder.readInt16(),
                errorMessage: decoder.readCompactString(),
                responses: decoder.readCompactArray((response) => ({
                    topicName: response.readCompactString()!,
                    topicId: response.readUUID(),
                    errorCode: response.readInt16(),
                    errorMessage: response.readCompactString(),
                    tags: response.readTagBuffer(),
                })),
                tags: decoder.readTagBuffer(),
            }),
    },
);
