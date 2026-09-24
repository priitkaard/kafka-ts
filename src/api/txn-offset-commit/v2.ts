import { createApi } from '../../utils/api';
import { TXN_OFFSET_COMMIT_V1 } from './v1';

/*
TxnOffsetCommit Request (Version: 2) => { transactional_id group_id producer_id producer_epoch [topics] }
  transactional_id => STRING
  group_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset committed_leader_epoch committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => NULLABLE_STRING

TxnOffsetCommit Response (Version: 2) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const TXN_OFFSET_COMMIT_V2 = createApi({
    ...TXN_OFFSET_COMMIT_V1,
    apiVersion: 2,
    fallback: TXN_OFFSET_COMMIT_V1,
    request: (encoder, data) =>
        encoder
            .writeString(data.transactionalId)
            .writeString(data.groupId)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeArray(data.topics, (encoder, topic) =>
                encoder.writeString(topic.name).writeArray(topic.partitions, (encoder, partition) =>
                    encoder
                        .writeInt32(partition.partitionIndex)
                        .writeInt64(partition.committedOffset)
                        .writeInt32(partition.committedLeaderEpoch ?? -1)
                        .writeString(partition.committedMetadata),
                ),
            ),
});
