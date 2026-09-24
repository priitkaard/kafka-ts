import { createApi } from '../../utils/api';
import { AUTHORIZED_OPERATIONS_OMITTED, MetadataRequest, MetadataResponse, throwIfError } from './common';

/*
Metadata Request (Version: 0) => { [topics] }
  topics => { name }
    name => STRING

Metadata Response (Version: 0) => { [brokers] [topics] }
  brokers => { node_id host port }
    node_id => INT32
    host => STRING
    port => INT32
  topics => { error_code name [partitions] }
    error_code => INT16
    name => STRING
    partitions => { error_code partition_index leader_id [replica_nodes] [isr_nodes] }
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
*/
export const METADATA_V0 = createApi<MetadataRequest, MetadataResponse>({
    apiKey: 3,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.topics ?? [], (encoder, topic) => encoder.writeString(topic.name)),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
            brokers: decoder.readArray((broker) => ({
                nodeId: broker.readInt32(),
                host: broker.readString()!,
                port: broker.readInt32(),
                rack: null,
            })),
            clusterId: null,
            controllerId: -1,
            topics: decoder.readArray((topic) => ({
                errorCode: topic.readInt16(),
                name: topic.readString()!,
                topicId: '',
                isInternal: false,
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
