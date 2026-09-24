import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { CREATE_TOPICS_V6 } from './v6';

/*
CreateTopics Request (Version: 7) => { (topics) timeout_ms validate_only }
  topics => { name num_partitions replication_factor (assignments) (configs) }
    name => COMPACT_STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => { partition_index (broker_ids) }
      partition_index => INT32
      broker_ids => INT32
    configs => { name value }
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN

CreateTopics Response (Version: 7) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name topic_id error_code error_message num_partitions replication_factor ?(configs) topic_config_error_code<tag: 0> }
    name => COMPACT_STRING
    topic_id => UUID
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    num_partitions => INT32
    replication_factor => INT16
    configs => { name value read_only config_source is_sensitive }
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
    topic_config_error_code<tag: 0> => INT16
*/
export const CREATE_TOPICS_V7 = createApi({
    ...CREATE_TOPICS_V6,
    apiVersion: 7,
    fallback: CREATE_TOPICS_V6,
    response: (decoder) =>
        throwIfError({
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
        }),
});
