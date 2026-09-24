import { createApi } from '../../utils/api';
import { OFFSET_COMMIT_V6 } from './v6';

/*
OffsetCommit Request (Version: 7) => { group_id generation_id_or_member_epoch member_id group_instance_id [topics] }
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  group_instance_id => NULLABLE_STRING
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset committed_leader_epoch committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => NULLABLE_STRING

OffsetCommit Response (Version: 7) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const OFFSET_COMMIT_V7 = createApi({
    ...OFFSET_COMMIT_V6,
    apiVersion: 7,
    fallback: OFFSET_COMMIT_V6,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.generationIdOrMemberEpoch)
            .writeString(data.memberId)
            .writeString(data.groupInstanceId)
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt64(partition.committedOffset)
                            .writeInt32(partition.committedLeaderEpoch)
                            .writeString(partition.committedMetadata),
                    ),
            ),
});
