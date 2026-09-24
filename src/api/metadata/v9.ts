import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { METADATA_V8 } from './v8';

/*
Metadata Request (Version: 9) => { ?(topics) allow_auto_topic_creation include_cluster_authorized_operations include_topic_authorized_operations }
  topics => { name }
    name => COMPACT_STRING
  allow_auto_topic_creation => BOOLEAN
  include_cluster_authorized_operations => BOOLEAN
  include_topic_authorized_operations => BOOLEAN

Metadata Response (Version: 9) => { throttle_time_ms (brokers) cluster_id controller_id (topics) cluster_authorized_operations }
  throttle_time_ms => INT32
  brokers => { node_id host port rack }
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => { error_code name is_internal (partitions) topic_authorized_operations }
    error_code => INT16
    name => COMPACT_STRING
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
  cluster_authorized_operations => INT32
*/
export const METADATA_V9 = createApi({
    ...METADATA_V8,
    apiVersion: 9,
    fallback: METADATA_V8,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics ?? null, (encoder, topic) =>
                encoder.writeCompactString(topic.name).writeTagBuffer(),
            )
            .writeBoolean(data.allowTopicAutoCreation ?? false)
            .writeBoolean(data.includeClusterAuthorizedOperations ?? false)
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
                topicId: '',
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
            clusterAuthorizedOperations: decoder.readInt32(),
            errorCode: 0,
            tags: decoder.readTagBuffer(),
        }),
});
