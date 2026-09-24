import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { OFFSET_FETCH_V4 } from './v4';

/*
OffsetFetch Request (Version: 5) => { group_id ?[topics] }
  group_id => STRING
  topics => { name [partition_indexes] }
    name => STRING
    partition_indexes => INT32

OffsetFetch Response (Version: 5) => { throttle_time_ms [topics] error_code }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset committed_leader_epoch metadata error_code }
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      metadata => NULLABLE_STRING
      error_code => INT16
  error_code => INT16
*/
export const OFFSET_FETCH_V5 = createApi({
    ...OFFSET_FETCH_V4,
    apiVersion: 5,
    fallback: OFFSET_FETCH_V4,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            groups: [
                {
                    groupId: '',
                    topics: decoder.readArray((topic) => ({
                        name: topic.readString()!,
                        partitions: topic.readArray((partition) => ({
                            partitionIndex: partition.readInt32(),
                            committedOffset: partition.readInt64(),
                            committedLeaderEpoch: partition.readInt32(),
                            committedMetadata: partition.readString(),
                            errorCode: partition.readInt16(),
                            tags: {},
                        })),
                        tags: {},
                    })),
                    errorCode: decoder.readInt16(),
                    tags: {},
                },
            ],
            tags: {},
        }),
});
