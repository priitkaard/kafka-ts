import { createApi } from '../../utils/api';
import { AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsResponse, throwIfError } from './common';

/*
AlterShareGroupOffsets Request (Version: 0) => { group_id (topics) }
  group_id => COMPACT_STRING
  topics => { topic_name (partitions) }
    topic_name => COMPACT_STRING
    partitions => { partition_index start_offset }
      partition_index => INT32
      start_offset => INT64

AlterShareGroupOffsets Response (Version: 0) => { throttle_time_ms error_code error_message (responses) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => { topic_name topic_id (partitions) }
    topic_name => COMPACT_STRING
    topic_id => UUID
    partitions => { partition_index error_code error_message }
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export const ALTER_SHARE_GROUP_OFFSETS_V0 = createApi<AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsResponse>({
    apiKey: 91,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.topicName)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition.partitionIndex).writeInt64(partition.startOffset).writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            responses: decoder.readCompactArray((response) => ({
                topicName: response.readCompactString()!,
                topicId: response.readUUID(),
                partitions: response.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    errorMessage: partition.readCompactString(),
                    tags: partition.readTagBuffer(),
                })),
                tags: response.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
