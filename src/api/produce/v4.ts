import { createApi } from '../../utils/api';
import { PRODUCE_V3 } from './v3';

/*
Produce Request (Version: 4) => { transactional_id acks timeout_ms [topic_data] }
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => { name [partition_data] }
    name => STRING
    partition_data => { index records }
      index => INT32
      records => NULLABLE_RECORDS

Produce Response (Version: 4) => { [responses] throttle_time_ms }
  responses => { name [partition_responses] }
    name => STRING
    partition_responses => { index error_code base_offset log_append_time_ms }
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
  throttle_time_ms => INT32
*/
export const PRODUCE_V4 = createApi({ ...PRODUCE_V3, apiVersion: 4, fallback: PRODUCE_V3 });
