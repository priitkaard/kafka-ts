import { createApi } from '../../utils/api';
import { ADD_PARTITIONS_TO_TXN_V4 } from './v4';

/*
AddPartitionsToTxn Request (Version: 5) => { (transactions) }
  transactions => { transactional_id producer_id producer_epoch verify_only (topics) }
    transactional_id => COMPACT_STRING
    producer_id => INT64
    producer_epoch => INT16
    verify_only => BOOLEAN
    topics => { name (partitions) }
      name => COMPACT_STRING
      partitions => INT32

AddPartitionsToTxn Response (Version: 5) => { throttle_time_ms error_code (results_by_transaction) }
  throttle_time_ms => INT32
  error_code => INT16
  results_by_transaction => { transactional_id (topic_results) }
    transactional_id => COMPACT_STRING
    topic_results => { name (results_by_partition) }
      name => COMPACT_STRING
      results_by_partition => { partition_index partition_error_code }
        partition_index => INT32
        partition_error_code => INT16
*/
export const ADD_PARTITIONS_TO_TXN_V5 = createApi({
    ...ADD_PARTITIONS_TO_TXN_V4,
    apiVersion: 5,
    fallback: ADD_PARTITIONS_TO_TXN_V4,
});
