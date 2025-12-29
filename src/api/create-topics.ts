import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type CreateTopicsRequest = {
    topics: {
        name: string;
        numPartitions?: number;
        replicationFactor?: number;
        assignments?: {
            partitionIndex: number;
            brokerIds: number[];
        }[];
        configs?: {
            name: string;
            value: string | null;
        }[];
    }[];
    timeoutMs?: number;
    validateOnly?: boolean;
};

type CreateTopicsResponse = {
    throttleTimeMs: number;
    topics: {
        name: string;
        _topicId: string;
        errorCode: number;
        errorMessage: string | null;
        _numPartitions: number;
        _replicationFactor: number;
        configs: {
            name: string;
            value: string | null;
            readOnly: boolean;
            configSource: number;
            isSensitive: boolean;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

/*
CreateTopics Request (Version: 2) => [topics] timeout_ms validate_only 
  topics => name num_partitions replication_factor [assignments] [configs] 
    name => STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] 
      partition_index => INT32
      broker_ids => INT32
    configs => name value 
      name => STRING
      value => NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN

CreateTopics Response (Version: 2) => throttle_time_ms [topics] 
  throttle_time_ms => INT32
  topics => name error_code error_message 
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
*/
const CREATE_TOPICS_V2 = createApi<CreateTopicsRequest, CreateTopicsResponse>({
    apiKey: 19,
    apiVersion: 2,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeInt32(topic.numPartitions ?? -1)
                    .writeInt16(topic.replicationFactor ?? -1)
                    .writeArray(topic.assignments ?? [], (encoder, assignment) =>
                        encoder
                            .writeInt32(assignment.partitionIndex)
                            .writeArray(assignment.brokerIds, (encoder, brokerId) => encoder.writeInt32(brokerId)),
                    )
                    .writeArray(topic.configs ?? [], (encoder, config) =>
                        encoder.writeString(config.name).writeString(config.value),
                    ),
            )
            .writeInt32(data.timeoutMs ?? 10_000)
            .writeBoolean(data.validateOnly ?? false),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readArray((topic) => ({
                name: topic.readString()!,
                _topicId: '', // TopicId not present in v2 response
                errorCode: topic.readInt16(),
                errorMessage: topic.readString(),
                _numPartitions: 0, // Not present in v2 response
                _replicationFactor: 0, // Not present in v2 response
                configs: [], // Not present in v2 response
                tags: {},
            })),
            tags: {},
        };
        result.topics.forEach((topic) => {
            if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, topic.errorMessage, result);
        });
        return result;
    },
});

/*
CreateTopics Request (Version: 7) => [topics] timeout_ms validate_only _tagged_fields 
  topics => name num_partitions replication_factor [assignments] [configs] _tagged_fields 
    name => COMPACT_STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => partition_index [broker_ids] _tagged_fields 
      partition_index => INT32
      broker_ids => INT32
    configs => name value _tagged_fields 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN

CreateTopics Response (Version: 7) => throttle_time_ms [topics] _tagged_fields 
  throttle_time_ms => INT32
  topics => name topic_id error_code error_message num_partitions replication_factor [configs] _tagged_fields 
    name => COMPACT_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    num_partitions => INT32
    replication_factor => INT16
    configs => name value read_only config_source is_sensitive _tagged_fields 
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
*/
export const CREATE_TOPICS = createApi<CreateTopicsRequest, CreateTopicsResponse>({
    apiKey: 19,
    apiVersion: 7,
    fallback: CREATE_TOPICS_V2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeInt32(topic.numPartitions ?? -1)
                    .writeInt16(topic.replicationFactor ?? -1)
                    .writeCompactArray(topic.assignments ?? [], (encoder, assignment) =>
                        encoder
                            .writeInt32(assignment.partitionIndex)
                            .writeCompactArray(assignment.brokerIds, (encoder, brokerId) =>
                                encoder.writeInt32(brokerId),
                            )
                            .writeTagBuffer(),
                    )
                    .writeCompactArray(topic.configs ?? [], (encoder, config) =>
                        encoder.writeCompactString(config.name).writeCompactString(config.value).writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeInt32(data.timeoutMs ?? 10_000)
            .writeBoolean(data.validateOnly ?? false)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                name: topic.readCompactString()!,
                _topicId: topic.readUUID(),
                errorCode: topic.readInt16(),
                errorMessage: topic.readCompactString(),
                _numPartitions: topic.readInt32(),
                _replicationFactor: topic.readInt16(),
                configs: topic.readCompactArray((config) => ({
                    name: config.readCompactString()!,
                    value: config.readCompactString(),
                    readOnly: config.readBoolean(),
                    configSource: config.readInt8(),
                    isSensitive: config.readBoolean(),
                    tags: config.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        result.topics.forEach((topic) => {
            if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, topic.errorMessage, result);
        });
        return result;
    },
});
