import { createApi } from '../../utils/api';
import { OFFSET_COMMIT_V4 } from './v4';

/*
OffsetCommit Request (Version: 5) => { group_id generation_id_or_member_epoch member_id [topics] }
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING

OffsetCommit Response (Version: 5) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const OFFSET_COMMIT_V5 = createApi({
    ...OFFSET_COMMIT_V4,
    apiVersion: 5,
    fallback: OFFSET_COMMIT_V4,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.generationIdOrMemberEpoch)
            .writeString(data.memberId)
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt64(partition.committedOffset)
                            .writeString(partition.committedMetadata),
                    ),
            ),
});
