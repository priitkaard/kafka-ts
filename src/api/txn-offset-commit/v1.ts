import { createApi } from '../../utils/api';
import { TXN_OFFSET_COMMIT_V0 } from './v0';

/*
TxnOffsetCommit Request (Version: 1) => { transactional_id group_id producer_id producer_epoch [topics] }
  transactional_id => STRING
  group_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING

TxnOffsetCommit Response (Version: 1) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const TXN_OFFSET_COMMIT_V1 = createApi({
    ...TXN_OFFSET_COMMIT_V0,
    apiVersion: 1,
    fallback: TXN_OFFSET_COMMIT_V0,
});
