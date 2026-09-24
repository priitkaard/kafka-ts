import { createApi } from '../../utils/api';
import { AUTHORIZED_OPERATIONS_OMITTED, throwIfError } from './common';
import { METADATA_V2 } from './v2';

/*
Metadata Request (Version: 3) => { ?[topics] }
  topics => { name }
    name => STRING

Metadata Response (Version: 3) => { throttle_time_ms [brokers] cluster_id controller_id [topics] }
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
export const METADATA_V3 = createApi({
    ...METADATA_V2,
    apiVersion: 3,
    fallback: METADATA_V2,
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
                    leaderEpoch: -1,
                    replicaNodes: partition.readArray((node) => node.readInt32()),
                    isrNodes: partition.readArray((node) => node.readInt32()),
                    offlineReplicas: [],
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
