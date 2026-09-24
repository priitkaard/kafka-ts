import { createApi } from '../../utils/api';
import { ADD_PARTITIONS_TO_TXN_V0 } from './v0';

/*
AddPartitionsToTxn Request (Version: 1) => { v3_and_below_transactional_id v3_and_below_producer_id v3_and_below_producer_epoch [v3_and_below_topics] }
  v3_and_below_transactional_id => STRING
  v3_and_below_producer_id => INT64
  v3_and_below_producer_epoch => INT16
  v3_and_below_topics => { name [partitions] }
    name => STRING
    partitions => INT32

AddPartitionsToTxn Response (Version: 1) => { throttle_time_ms [results_by_topic_v3_and_below] }
  throttle_time_ms => INT32
  results_by_topic_v3_and_below => { name [results_by_partition] }
    name => STRING
    results_by_partition => { partition_index partition_error_code }
      partition_index => INT32
      partition_error_code => INT16
*/
export const ADD_PARTITIONS_TO_TXN_V1 = createApi({
    ...ADD_PARTITIONS_TO_TXN_V0,
    apiVersion: 1,
    fallback: ADD_PARTITIONS_TO_TXN_V0,
});
