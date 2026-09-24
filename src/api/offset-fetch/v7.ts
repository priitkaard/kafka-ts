import { createApi } from '../../utils/api';
import { getSingleGroup } from './common';
import { OFFSET_FETCH_V6 } from './v6';

/*
OffsetFetch Request (Version: 7) => { group_id ?(topics) require_stable }
  group_id => COMPACT_STRING
  topics => { name (partition_indexes) }
    name => COMPACT_STRING
    partition_indexes => INT32
  require_stable => BOOLEAN

OffsetFetch Response (Version: 7) => { throttle_time_ms (topics) error_code }
  throttle_time_ms => INT32
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
export const OFFSET_FETCH_V7 = createApi({
    ...OFFSET_FETCH_V6,
    apiVersion: 7,
    fallback: OFFSET_FETCH_V6,
    request: (encoder, data) => {
        const group = getSingleGroup(data);
        return encoder
            .writeCompactString(group.groupId)
            .writeCompactArray(group.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                        encoder.writeInt32(partitionIndex),
                    )
                    .writeTagBuffer(),
            )
            .writeBoolean(data.requireStable)
            .writeTagBuffer();
    },
});
