import { createApi } from '../../utils/api';
import { LIST_OFFSETS_V6 } from './v6';

/*
ListOffsets Request (Version: 7) => { replica_id isolation_level (topics) }
  replica_id => INT32
  isolation_level => INT8
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index current_leader_epoch timestamp }
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64

ListOffsets Response (Version: 7) => { throttle_time_ms (topics) }
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
export const LIST_OFFSETS_V7 = createApi({ ...LIST_OFFSETS_V6, apiVersion: 7, fallback: LIST_OFFSETS_V6 });
