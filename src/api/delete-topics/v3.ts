import { createApi } from '../../utils/api';
import { DELETE_TOPICS_V2 } from './v2';

/*
DeleteTopics Request (Version: 3) => { [topic_names] timeout_ms }
  topic_names => STRING
  timeout_ms => INT32

DeleteTopics Response (Version: 3) => { throttle_time_ms [responses] }
  throttle_time_ms => INT32
  responses => { name error_code }
    name => STRING
    error_code => INT16
*/
export const DELETE_TOPICS_V3 = createApi({ ...DELETE_TOPICS_V2, apiVersion: 3, fallback: DELETE_TOPICS_V2 });
