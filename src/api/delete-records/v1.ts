import { createApi } from '../../utils/api';
import { DELETE_RECORDS_V0 } from './v0';

/*
DeleteRecords Request (Version: 1) => { [topics] timeout_ms }
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index offset }
      partition_index => INT32
      offset => INT64
  timeout_ms => INT32

DeleteRecords Response (Version: 1) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index low_watermark error_code }
      partition_index => INT32
      low_watermark => INT64
      error_code => INT16
*/
export const DELETE_RECORDS_V1 = createApi({ ...DELETE_RECORDS_V0, apiVersion: 1, fallback: DELETE_RECORDS_V0 });
