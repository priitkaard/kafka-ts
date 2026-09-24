import { createApi } from '../../utils/api';
import { OFFSET_FETCH_V8 } from './v8';

/*
OffsetFetch Request (Version: 9) => { (groups) require_stable }
  groups => { group_id member_id member_epoch ?(topics) }
    group_id => COMPACT_STRING
    member_id => COMPACT_NULLABLE_STRING
    member_epoch => INT32
    topics => { name (partition_indexes) }
      name => COMPACT_STRING
      partition_indexes => INT32
  require_stable => BOOLEAN

OffsetFetch Response (Version: 9) => { throttle_time_ms (groups) }
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
export const OFFSET_FETCH_V9 = createApi({
    ...OFFSET_FETCH_V8,
    apiVersion: 9,
    fallback: OFFSET_FETCH_V8,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.groups, (encoder, group) =>
                encoder
                    .writeCompactString(group.groupId)
                    .writeCompactString(group.memberId ?? null)
                    .writeInt32(group.memberEpoch ?? -1)
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
});
