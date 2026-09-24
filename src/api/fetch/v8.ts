import { createApi } from '../../utils/api';
import { FETCH_V7 } from './v7';

/*
Fetch Request (Version: 8) => { replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] }
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => { topic [partitions] }
    topic => STRING
    partitions => { partition fetch_offset log_start_offset partition_max_bytes }
      partition => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => { topic [partitions] }
    topic => STRING
    partitions => INT32

Fetch Response (Version: 8) => { throttle_time_ms error_code session_id [responses] }
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => { topic [partitions] }
    topic => STRING
    partitions => { partition_index error_code high_watermark last_stable_offset log_start_offset ?[aborted_transactions] records }
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => { producer_id first_offset }
        producer_id => INT64
        first_offset => INT64
      records => NULLABLE_RECORDS
*/
export const FETCH_V8 = createApi({ ...FETCH_V7, apiVersion: 8, fallback: FETCH_V7 });
