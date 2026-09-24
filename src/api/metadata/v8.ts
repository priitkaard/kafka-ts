import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { METADATA_V7 } from './v7';

/*
Metadata Request (Version: 8) => { ?[topics] allow_auto_topic_creation include_cluster_authorized_operations include_topic_authorized_operations }
  topics => { name }
    name => STRING
  allow_auto_topic_creation => BOOLEAN
  include_cluster_authorized_operations => BOOLEAN
  include_topic_authorized_operations => BOOLEAN

Metadata Response (Version: 8) => { throttle_time_ms [brokers] cluster_id controller_id [topics] cluster_authorized_operations }
  throttle_time_ms => INT32
  brokers => { node_id host port rack }
    node_id => INT32
    host => STRING
    port => INT32
    rack => NULLABLE_STRING
  cluster_id => NULLABLE_STRING
  controller_id => INT32
  topics => { error_code name is_internal [partitions] topic_authorized_operations }
    error_code => INT16
    name => STRING
    is_internal => BOOLEAN
    partitions => { error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] }
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
export const METADATA_V8 = createApi({
    ...METADATA_V7,
    apiVersion: 8,
    fallback: METADATA_V7,
    request: (encoder, data) =>
        encoder
            .writeArray(data.topics ?? null, (encoder, topic) => encoder.writeString(topic.name))
            .writeBoolean(data.allowTopicAutoCreation ?? false)
            .writeBoolean(data.includeClusterAuthorizedOperations ?? false)
            .writeBoolean(data.includeTopicAuthorizedOperations ?? false),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            brokers: decoder.readArray((broker) => ({
                nodeId: broker.readInt32(),
                host: broker.readString()!,
                port: broker.readInt32(),
                rack: broker.readString(),
            })),
            clusterId: decoder.readString(),
            controllerId: decoder.readInt32(),
            topics: decoder.readArray((topic) => ({
                errorCode: topic.readInt16(),
                name: topic.readString()!,
                topicId: '',
                isInternal: topic.readBoolean(),
                partitions: topic.readArray((partition) => ({
                    errorCode: partition.readInt16(),
                    partitionIndex: partition.readInt32(),
                    leaderId: partition.readInt32(),
                    leaderEpoch: partition.readInt32(),
                    replicaNodes: partition.readArray((node) => node.readInt32()),
                    isrNodes: partition.readArray((node) => node.readInt32()),
                    offlineReplicas: partition.readArray((node) => node.readInt32()),
                    tags: {},
                })),
                topicAuthorizedOperations: topic.readInt32(),
                tags: {},
            })),
            clusterAuthorizedOperations: decoder.readInt32(),
            errorCode: 0,
            tags: {},
        }),
});
