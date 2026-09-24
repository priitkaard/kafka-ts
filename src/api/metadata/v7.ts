import { createApi } from '../../utils/api';
import { AUTHORIZED_OPERATIONS_OMITTED, throwIfError } from './common';
import { METADATA_V6 } from './v6';

/*
Metadata Request (Version: 7) => { ?[topics] allow_auto_topic_creation }
  topics => { name }
    name => STRING
  allow_auto_topic_creation => BOOLEAN

Metadata Response (Version: 7) => { throttle_time_ms [brokers] cluster_id controller_id [topics] }
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
    partitions => { error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] }
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
*/
export const METADATA_V7 = createApi({
    ...METADATA_V6,
    apiVersion: 7,
    fallback: METADATA_V6,
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
                topicAuthorizedOperations: AUTHORIZED_OPERATIONS_OMITTED,
                tags: {},
            })),
            clusterAuthorizedOperations: AUTHORIZED_OPERATIONS_OMITTED,
            errorCode: 0,
            tags: {},
        }),
});
