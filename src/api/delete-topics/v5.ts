import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DELETE_TOPICS_V4 } from './v4';

/*
DeleteTopics Request (Version: 5) => { (topic_names) timeout_ms }
  topic_names => COMPACT_STRING
  timeout_ms => INT32

DeleteTopics Response (Version: 5) => { throttle_time_ms (responses) }
  throttle_time_ms => INT32
  responses => { name error_code error_message }
    name => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const DELETE_TOPICS_V5 = createApi({
    ...DELETE_TOPICS_V4,
    apiVersion: 5,
    fallback: DELETE_TOPICS_V4,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readCompactArray((response) => ({
                name: response.readCompactString()!,
                _topicId: '',
                errorCode: response.readInt16(),
                errorMessage: response.readCompactString(),
                tags: response.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
