import { createApi } from '../../utils/api';
import { TXN_OFFSET_COMMIT_V3 } from './v3';

/*
TxnOffsetCommit Request (Version: 4) => { transactional_id group_id producer_id producer_epoch generation_id member_id group_instance_id (topics) }
  transactional_id => COMPACT_STRING
  group_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index committed_offset committed_leader_epoch committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING

TxnOffsetCommit Response (Version: 4) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const TXN_OFFSET_COMMIT_V4 = createApi({
    ...TXN_OFFSET_COMMIT_V3,
    apiVersion: 4,
    fallback: TXN_OFFSET_COMMIT_V3,
});
