import { createApi } from '../../utils/api';
import { DELETE_TOPICS_V1 } from './v1';

/*
DeleteTopics Request (Version: 2) => { [topic_names] timeout_ms }
  topic_names => STRING
  timeout_ms => INT32

DeleteTopics Response (Version: 2) => { throttle_time_ms [responses] }
  throttle_time_ms => INT32
  responses => { name error_code }
    name => STRING
    error_code => INT16
*/
export const DELETE_TOPICS_V2 = createApi({ ...DELETE_TOPICS_V1, apiVersion: 2, fallback: DELETE_TOPICS_V1 });
