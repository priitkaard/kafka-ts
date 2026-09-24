import { createApi } from '../../utils/api';
import { getSingleGroup, throwIfError } from './common';
import { OFFSET_FETCH_V5 } from './v5';

/*
OffsetFetch Request (Version: 6) => { group_id ?(topics) }
  group_id => COMPACT_STRING
  topics => { name (partition_indexes) }
    name => COMPACT_STRING
    partition_indexes => INT32

OffsetFetch Response (Version: 6) => { throttle_time_ms (topics) error_code }
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
export const OFFSET_FETCH_V6 = createApi({
    ...OFFSET_FETCH_V5,
    apiVersion: 6,
    fallback: OFFSET_FETCH_V5,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
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
            .writeTagBuffer();
    },
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            groups: [
                {
                    groupId: '',
                    topics: decoder.readCompactArray((topic) => ({
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
                    errorCode: decoder.readInt16(),
                    tags: {},
                },
            ],
            tags: decoder.readTagBuffer(),
        }),
});
