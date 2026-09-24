import { createApi } from '../../utils/api';
import { OFFSET_COMMIT_V8 } from './v8';

/*
OffsetCommit Request (Version: 9) => { group_id generation_id_or_member_epoch member_id group_instance_id (topics) }
  group_id => COMPACT_STRING
  generation_id_or_member_epoch => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index committed_offset committed_leader_epoch committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING

OffsetCommit Response (Version: 9) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const OFFSET_COMMIT_V9 = createApi({ ...OFFSET_COMMIT_V8, apiVersion: 9, fallback: OFFSET_COMMIT_V8 });
