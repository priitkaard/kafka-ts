import { createApi } from '../../utils/api';
import { OFFSET_FETCH_V3 } from './v3';

/*
OffsetFetch Request (Version: 4) => { group_id ?[topics] }
  group_id => STRING
  topics => { name [partition_indexes] }
    name => STRING
    partition_indexes => INT32

OffsetFetch Response (Version: 4) => { throttle_time_ms [topics] error_code }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset metadata error_code }
      partition_index => INT32
      committed_offset => INT64
      metadata => NULLABLE_STRING
      error_code => INT16
  error_code => INT16
*/
export const OFFSET_FETCH_V4 = createApi({ ...OFFSET_FETCH_V3, apiVersion: 4, fallback: OFFSET_FETCH_V3 });
