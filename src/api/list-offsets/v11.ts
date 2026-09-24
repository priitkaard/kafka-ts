import { createApi } from '../../utils/api';
import { LIST_OFFSETS_V10 } from './v10';

/*
ListOffsets Request (Version: 11) => { replica_id isolation_level (topics) timeout_ms }
  replica_id => INT32
  isolation_level => INT8
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index current_leader_epoch timestamp }
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
  timeout_ms => INT32

ListOffsets Response (Version: 11) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code timestamp offset leader_epoch }
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
*/
export const LIST_OFFSETS_V11 = createApi({ ...LIST_OFFSETS_V10, apiVersion: 11, fallback: LIST_OFFSETS_V10 });
