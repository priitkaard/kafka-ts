import { createApi } from '../../utils/api';
import { DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsResponse, throwIfError } from './common';

/*
DescribeShareGroupOffsets Request (Version: 0) => { (groups) }
  groups => { group_id ?(topics) }
    group_id => COMPACT_STRING
    topics => { topic_name (partitions) }
      topic_name => COMPACT_STRING
      partitions => INT32

DescribeShareGroupOffsets Response (Version: 0) => { throttle_time_ms (groups) }
  throttle_time_ms => INT32
  groups => { group_id (topics) error_code error_message }
    group_id => COMPACT_STRING
    topics => { topic_name topic_id (partitions) }
      topic_name => COMPACT_STRING
      topic_id => UUID
      partitions => { partition_index start_offset leader_epoch error_code error_message }
        partition_index => INT32
        start_offset => INT64
        leader_epoch => INT32
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const DESCRIBE_SHARE_GROUP_OFFSETS_V0 = createApi<
    DescribeShareGroupOffsetsRequest,
    DescribeShareGroupOffsetsResponse
>({
    apiKey: 90,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.groups, (encoder, group) =>
                encoder
                    .writeCompactString(group.groupId)
                    .writeCompactArray(group.topics, (encoder, topic) =>
                        encoder
                            .writeCompactString(topic.topicName)
                            .writeCompactArray(topic.partitions, (encoder, partition) => encoder.writeInt32(partition))
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            groups: decoder.readCompactArray((group) => ({
                groupId: group.readCompactString()!,
                topics: group.readCompactArray((topic) => ({
                    topicName: topic.readCompactString()!,
                    topicId: topic.readUUID(),
                    partitions: topic.readCompactArray((partition) => ({
                        partitionIndex: partition.readInt32(),
                        startOffset: partition.readInt64(),
                        leaderEpoch: partition.readInt32(),
                        lag: -1n,
                        errorCode: partition.readInt16(),
                        errorMessage: partition.readCompactString(),
                        tags: partition.readTagBuffer(),
                    })),
                    tags: topic.readTagBuffer(),
                })),
                errorCode: group.readInt16(),
                errorMessage: group.readCompactString(),
                tags: group.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
