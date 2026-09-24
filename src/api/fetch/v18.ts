import { createApi } from '../../utils/api';
import { FETCH_V17 } from './v17';

/*
Fetch Request (Version: 18) => { max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch (topics) (forgotten_topics_data) rack_id cluster_id<tag: 0> replica_state<tag: 1> }
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes replica_directory_id<tag: 0> high_watermark<tag: 1> }
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
      replica_directory_id<tag: 0> => UUID
      high_watermark<tag: 1> => INT64
  forgotten_topics_data => { topic_id (partitions) }
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING
  replica_state<tag: 1> => { replica_id replica_epoch }
    replica_id => INT32
    replica_epoch => INT64

Fetch Response (Version: 18) => { throttle_time_ms error_code session_id (responses) node_endpoints<tag: 0> }
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index error_code high_watermark last_stable_offset log_start_offset ?(aborted_transactions) preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> }
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => { producer_id first_offset }
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_NULLABLE_RECORDS
      diverging_epoch<tag: 0> => { epoch end_offset }
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => { leader_id leader_epoch }
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => { end_offset epoch }
        end_offset => INT64
        epoch => INT32
  node_endpoints<tag: 0> => { node_id host port rack }
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
*/
export const FETCH_V18 = createApi({ ...FETCH_V17, apiVersion: 18, fallback: FETCH_V17 });
