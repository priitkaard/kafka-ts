import { createApi } from '../../utils/api';
import { CreateTopicsRequest, CreateTopicsResponse, throwIfError } from './common';

/*
CreateTopics Request (Version: 2) => { [topics] timeout_ms validate_only }
  topics => { name num_partitions replication_factor [assignments] [configs] }
    name => STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => { partition_index [broker_ids] }
      partition_index => INT32
      broker_ids => INT32
    configs => { name value }
      name => STRING
      value => NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN

CreateTopics Response (Version: 2) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name error_code error_message }
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
*/
export const CREATE_TOPICS_V2 = createApi<CreateTopicsRequest, CreateTopicsResponse>({
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
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readArray((topic) => ({
                name: topic.readString()!,
                _topicId: '',
                errorCode: topic.readInt16(),
                errorMessage: topic.readString(),
                _numPartitions: 0,
                _replicationFactor: 0,
                configs: [],
                tags: {},
            })),
            tags: {},
        }),
});
