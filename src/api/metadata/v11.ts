import { createApi } from '../../utils/api';
import { AUTHORIZED_OPERATIONS_OMITTED, throwIfError } from './common';
import { METADATA_V10 } from './v10';

/*
Metadata Request (Version: 11) => { ?(topics) allow_auto_topic_creation include_topic_authorized_operations }
  topics => { topic_id name }
    topic_id => UUID
    name => COMPACT_NULLABLE_STRING
  allow_auto_topic_creation => BOOLEAN
  include_topic_authorized_operations => BOOLEAN

Metadata Response (Version: 11) => { throttle_time_ms (brokers) cluster_id controller_id (topics) }
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
    name => COMPACT_STRING
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
export const METADATA_V11 = createApi({
    ...METADATA_V10,
    apiVersion: 11,
    fallback: METADATA_V10,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics ?? null, (encoder, topic) =>
                encoder.writeUUID(topic.id).writeCompactString(topic.name).writeTagBuffer(),
            )
            .writeBoolean(data.allowTopicAutoCreation ?? false)
            .writeBoolean(data.includeTopicAuthorizedOperations ?? false)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            brokers: decoder.readCompactArray((broker) => ({
                nodeId: broker.readInt32(),
                host: broker.readCompactString()!,
                port: broker.readInt32(),
                rack: broker.readCompactString(),
                tags: broker.readTagBuffer(),
            })),
            clusterId: decoder.readCompactString(),
            controllerId: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                errorCode: topic.readInt16(),
                name: topic.readCompactString()!,
                topicId: topic.readUUID(),
                isInternal: topic.readBoolean(),
                partitions: topic.readCompactArray((partition) => ({
                    errorCode: partition.readInt16(),
                    partitionIndex: partition.readInt32(),
                    leaderId: partition.readInt32(),
                    leaderEpoch: partition.readInt32(),
                    replicaNodes: partition.readCompactArray((node) => node.readInt32()),
                    isrNodes: partition.readCompactArray((node) => node.readInt32()),
                    offlineReplicas: partition.readCompactArray((node) => node.readInt32()),
                    tags: partition.readTagBuffer(),
                })),
                topicAuthorizedOperations: topic.readInt32(),
                tags: topic.readTagBuffer(),
            })),
            clusterAuthorizedOperations: AUTHORIZED_OPERATIONS_OMITTED,
            errorCode: 0,
            tags: decoder.readTagBuffer(),
        }),
});
