import { createApi } from '../../utils/api';
import { PRODUCE_V6 } from './v6';

/*
Produce Request (Version: 7) => { transactional_id acks timeout_ms [topic_data] }
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => { name [partition_data] }
    name => STRING
    partition_data => { index records }
      index => INT32
      records => NULLABLE_RECORDS

Produce Response (Version: 7) => { [responses] throttle_time_ms }
  responses => { name [partition_responses] }
    name => STRING
    partition_responses => { index error_code base_offset log_append_time_ms log_start_offset }
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
  throttle_time_ms => INT32
*/
export const PRODUCE_V7 = createApi({ ...PRODUCE_V6, apiVersion: 7, fallback: PRODUCE_V6 });
