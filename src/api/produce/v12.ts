import { createApi } from '../../utils/api';
import { PRODUCE_V11 } from './v11';

/*
Produce Request (Version: 12) => { transactional_id acks timeout_ms (topic_data) }
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => { name (partition_data) }
    name => COMPACT_STRING
    partition_data => { index records }
      index => INT32
      records => COMPACT_NULLABLE_RECORDS

Produce Response (Version: 12) => { (responses) throttle_time_ms node_endpoints<tag: 0> }
  responses => { name (partition_responses) }
    name => COMPACT_STRING
    partition_responses => { index error_code base_offset log_append_time_ms log_start_offset (record_errors) error_message current_leader<tag: 0> }
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => { batch_index batch_index_error_message }
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
      current_leader<tag: 0> => { leader_id leader_epoch }
        leader_id => INT32
        leader_epoch => INT32
  throttle_time_ms => INT32
  node_endpoints<tag: 0> => { node_id host port rack }
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
*/
export const PRODUCE_V12 = createApi({ ...PRODUCE_V11, apiVersion: 12, fallback: PRODUCE_V11 });
