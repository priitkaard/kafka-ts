import { createApi } from '../../utils/api';
import { METADATA_V5 } from './v5';

/*
Metadata Request (Version: 6) => { ?[topics] allow_auto_topic_creation }
  topics => { name }
    name => STRING
  allow_auto_topic_creation => BOOLEAN

Metadata Response (Version: 6) => { throttle_time_ms [brokers] cluster_id controller_id [topics] }
  throttle_time_ms => INT32
  brokers => { node_id host port rack }
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => { error_code name is_internal [partitions] }
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => { error_code partition_index leader_id [replica_nodes] [isr_nodes] [offline_replicas] }
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
*/
export const METADATA_V6 = createApi({ ...METADATA_V5, apiVersion: 6, fallback: METADATA_V5 });
