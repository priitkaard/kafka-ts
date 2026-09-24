import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { OFFSET_FETCH_V7 } from './v7';

/*
OffsetFetch Request (Version: 8) => { (groups) require_stable }
  groups => { group_id ?(topics) }
    group_id => COMPACT_STRING
    topics => { name (partition_indexes) }
      name => COMPACT_STRING
      partition_indexes => INT32
  require_stable => BOOLEAN

OffsetFetch Response (Version: 8) => { throttle_time_ms (groups) }
  throttle_time_ms => INT32
  groups => { group_id (topics) error_code }
    group_id => COMPACT_STRING
    topics => { name (partitions) }
      name => COMPACT_STRING
      partitions => { partition_index committed_offset committed_leader_epoch metadata error_code }
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        metadata => COMPACT_NULLABLE_STRING
        error_code => INT16
    error_code => INT16
*/
export const OFFSET_FETCH_V8 = createApi({
    ...OFFSET_FETCH_V7,
    apiVersion: 8,
    fallback: OFFSET_FETCH_V7,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.groups, (encoder, group) =>
                encoder
                    .writeCompactString(group.groupId)
                    .writeCompactArray(group.topics, (encoder, topic) =>
                        encoder
                            .writeCompactString(topic.name)
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
                    name: topic.readCompactString()!,
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
