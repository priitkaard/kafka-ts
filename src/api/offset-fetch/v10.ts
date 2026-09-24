import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { OFFSET_FETCH_V9 } from './v9';

/*
OffsetFetch Request (Version: 10) => { (groups) require_stable }
  groups => { group_id member_id member_epoch ?(topics) }
    group_id => COMPACT_STRING
    member_id => COMPACT_NULLABLE_STRING
    member_epoch => INT32
    topics => { topic_id (partition_indexes) }
      topic_id => UUID
      partition_indexes => INT32
  require_stable => BOOLEAN

OffsetFetch Response (Version: 10) => { throttle_time_ms (groups) }
  throttle_time_ms => INT32
  groups => { group_id (topics) error_code }
    group_id => COMPACT_STRING
    topics => { topic_id (partitions) }
      topic_id => UUID
      partitions => { partition_index committed_offset committed_leader_epoch metadata error_code }
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        metadata => COMPACT_NULLABLE_STRING
        error_code => INT16
    error_code => INT16
*/
export const OFFSET_FETCH_V10 = createApi({
    ...OFFSET_FETCH_V9,
    apiVersion: 10,
    fallback: OFFSET_FETCH_V9,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.groups, (encoder, group) =>
                encoder
                    .writeCompactString(group.groupId)
                    .writeCompactString(group.memberId ?? null)
                    .writeInt32(group.memberEpoch ?? -1)
                    .writeCompactArray(group.topics, (encoder, topic) =>
                        encoder
                            .writeUUID(topic.topicId)
                            .writeCompactArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                                encoder.writeInt32(partitionIndex),
                            )
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeBoolean(data.requireStable)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            groups: decoder.readCompactArray((group) => ({
                groupId: group.readCompactString()!,
                topics: group.readCompactArray((topic) => ({
                    topicId: topic.readUUID(),
                    partitions: topic.readCompactArray((partition) => ({
                        partitionIndex: partition.readInt32(),
                        committedOffset: partition.readInt64(),
                        committedLeaderEpoch: partition.readInt32(),
                        committedMetadata: partition.readCompactString(),
                        errorCode: partition.readInt16(),
                        tags: partition.readTagBuffer(),
                    })),
                    tags: topic.readTagBuffer(),
                })),
                errorCode: group.readInt16(),
                tags: group.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
