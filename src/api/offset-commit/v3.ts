import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { OFFSET_COMMIT_V2 } from './v2';

/*
OffsetCommit Request (Version: 3) => { group_id generation_id_or_member_epoch member_id retention_time_ms [topics] }
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  retention_time_ms => INT64
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING

OffsetCommit Response (Version: 3) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const OFFSET_COMMIT_V3 = createApi({
    ...OFFSET_COMMIT_V2,
    apiVersion: 3,
    fallback: OFFSET_COMMIT_V2,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readArray((topic) => ({
                name: topic.readString()!,
                partitions: topic.readArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
