import { createApi } from '../../utils/api';
import { METADATA_V3 } from './v3';

/*
Metadata Request (Version: 4) => { ?[topics] allow_auto_topic_creation }
  topics => { name }
    name => STRING
  allow_auto_topic_creation => BOOLEAN

Metadata Response (Version: 4) => { throttle_time_ms [brokers] cluster_id controller_id [topics] }
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
    partitions => { error_code partition_index leader_id [replica_nodes] [isr_nodes] }
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
*/
export const METADATA_V4 = createApi({
    ...METADATA_V3,
    apiVersion: 4,
    fallback: METADATA_V3,
    request: (encoder, data) =>
        encoder
            .writeArray(data.topics ?? null, (encoder, topic) => encoder.writeString(topic.name))
            .writeBoolean(data.allowTopicAutoCreation ?? false),
});
