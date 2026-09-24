import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { OFFSET_COMMIT_V7 } from './v7';

/*
OffsetCommit Request (Version: 8) => { group_id generation_id_or_member_epoch member_id group_instance_id (topics) }
  group_id => COMPACT_STRING
  generation_id_or_member_epoch => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index committed_offset committed_leader_epoch committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING

OffsetCommit Response (Version: 8) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const OFFSET_COMMIT_V8 = createApi({
    ...OFFSET_COMMIT_V7,
    apiVersion: 8,
    fallback: OFFSET_COMMIT_V7,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeInt32(data.generationIdOrMemberEpoch)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt64(partition.committedOffset)
                            .writeInt32(partition.committedLeaderEpoch)
                            .writeCompactString(partition.committedMetadata)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                name: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
