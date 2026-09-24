import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_SHARE_GROUP_OFFSETS_V0 } from './v0';

/*
DescribeShareGroupOffsets Request (Version: 1) => { (groups) }
  groups => { group_id ?(topics) }
    group_id => COMPACT_STRING
    topics => { topic_name (partitions) }
      topic_name => COMPACT_STRING
      partitions => INT32

DescribeShareGroupOffsets Response (Version: 1) => { throttle_time_ms (groups) }
  throttle_time_ms => INT32
  groups => { group_id (topics) error_code error_message }
    group_id => COMPACT_STRING
    topics => { topic_name topic_id (partitions) }
      topic_name => COMPACT_STRING
      topic_id => UUID
      partitions => { partition_index start_offset leader_epoch lag error_code error_message }
        partition_index => INT32
        start_offset => INT64
        leader_epoch => INT32
        lag => INT64
        error_code => INT16
        error_message => COMPACT_NULLABLE_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const DESCRIBE_SHARE_GROUP_OFFSETS_V1 = createApi({
    ...DESCRIBE_SHARE_GROUP_OFFSETS_V0,
    apiVersion: 1,
    fallback: DESCRIBE_SHARE_GROUP_OFFSETS_V0,
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
                        lag: partition.readInt64(),
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
