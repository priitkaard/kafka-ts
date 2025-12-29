import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type MetadataRequest = {
    topics?: { id: string | null; name: string }[] | null;
    allowTopicAutoCreation?: boolean;
    includeTopicAuthorizedOperations?: boolean;
};

type MetadataResponse = {
    throttleTimeMs: number;
    brokers: {
        nodeId: number;
        host: string;
        port: number;
        rack: string | null;
    }[];
    clusterId: string | null;
    controllerId: number;
    topics: {
        errorCode: number;
        name: string;
        topicId: string;
        isInternal: boolean;
        partitions: {
            errorCode: number;
            partitionIndex: number;
            leaderId: number;
            leaderEpoch: number;
            replicaNodes: number[];
            isrNodes: number[];
            offlineReplicas: number[];
            tags: Record<number, Buffer>;
        }[];
        topicAuthorizedOperations: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};
export type Metadata = MetadataResponse;

/*
Metadata Request (Version: 0) => [topics] 
  topics => name 
    name => STRING

Metadata Response (Version: 0) => [brokers] [topics] 
  brokers => node_id host port 
    node_id => INT32
    host => STRING
    port => INT32
  topics => error_code name [partitions] 
    error_code => INT16
    name => STRING
    partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes] 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      replica_nodes => INT32
      isr_nodes => INT32
*/
const METADATA_V0 = createApi<MetadataRequest, MetadataResponse>({
    apiKey: 3,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.topics ?? [], (encoder, topic) => encoder.writeString(topic.name)),
    response: (decoder) => {
        const result = {
            throttleTimeMs: 0,
            brokers: decoder.readArray((decoder) => ({
                nodeId: decoder.readInt32(),
                host: decoder.readString()!,
                port: decoder.readInt32(),
                rack: null,
            })),
            clusterId: null,
            controllerId: -1,
            topics: decoder.readArray((decoder) => ({
                errorCode: decoder.readInt16(),
                name: decoder.readString()!,
                topicId: '',
                isInternal: false,
                partitions: decoder.readArray((decoder) => ({
                    errorCode: decoder.readInt16(),
                    partitionIndex: decoder.readInt32(),
                    leaderId: decoder.readInt32(),
                    leaderEpoch: -1,
                    replicaNodes: decoder.readArray(() => decoder.readInt32()),
                    isrNodes: decoder.readArray(() => decoder.readInt32()),
                    offlineReplicas: [],
                    tags: {},
                })),
                topicAuthorizedOperations: -1,
                tags: {},
            })),
            tags: {},
        };
        result.topics.forEach((topic) => {
            if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, null, result);
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});

/*
Metadata Request (Version: 12) => [topics] allow_auto_topic_creation include_topic_authorized_operations _tagged_fields 
  topics => topic_id name _tagged_fields 
    topic_id => UUID
    name => COMPACT_NULLABLE_STRING
  allow_auto_topic_creation => BOOLEAN
  include_topic_authorized_operations => BOOLEAN

Metadata Response (Version: 12) => throttle_time_ms [brokers] cluster_id controller_id [topics] _tagged_fields 
  throttle_time_ms => INT32
  brokers => node_id host port rack _tagged_fields 
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_NULLABLE_STRING
  controller_id => INT32
  topics => error_code name topic_id is_internal [partitions] topic_authorized_operations _tagged_fields 
    error_code => INT16
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas] _tagged_fields 
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
*/
export const METADATA = createApi<MetadataRequest, MetadataResponse>({
    apiKey: 3,
    apiVersion: 12,
    fallback: METADATA_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics ?? null, (encoder, topic) =>
                encoder.writeUUID(topic.id).writeCompactString(topic.name).writeTagBuffer(),
            )
            .writeBoolean(data.allowTopicAutoCreation ?? false)
            .writeBoolean(data.includeTopicAuthorizedOperations ?? false)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
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
            tags: decoder.readTagBuffer(),
        };
        result.topics.forEach((topic) => {
            if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, null, result);
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});
