import { createApi } from '../../utils/api';
import { METADATA_V11 } from './v11';

/*
Metadata Request (Version: 12) => { ?(topics) allow_auto_topic_creation include_topic_authorized_operations }
  topics => { topic_id name }
    topic_id => UUID
    name => COMPACT_NULLABLE_STRING
  allow_auto_topic_creation => BOOLEAN
  include_topic_authorized_operations => BOOLEAN

Metadata Response (Version: 12) => { throttle_time_ms (brokers) cluster_id controller_id (topics) }
  throttle_time_ms => INT32
  brokers => { node_id host port rack }
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => { error_code name topic_id is_internal (partitions) topic_authorized_operations }
    error_code => INT16
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => { error_code partition_index leader_id leader_epoch (replica_nodes) (isr_nodes) (offline_replicas) }
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
*/
export const METADATA_V12 = createApi({ ...METADATA_V11, apiVersion: 12, fallback: METADATA_V11 });
