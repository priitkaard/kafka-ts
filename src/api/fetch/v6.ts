import { createApi } from '../../utils/api';
import { FETCH_V5 } from './v5';

/*
Fetch Request (Version: 6) => { replica_id max_wait_ms min_bytes max_bytes isolation_level [topics] }
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  topics => { topic [partitions] }
    topic => STRING
    partitions => { partition fetch_offset log_start_offset partition_max_bytes }
      partition => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32

Fetch Response (Version: 6) => { throttle_time_ms [responses] }
  throttle_time_ms => INT32
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
export const FETCH_V6 = createApi({ ...FETCH_V5, apiVersion: 6, fallback: FETCH_V5 });
