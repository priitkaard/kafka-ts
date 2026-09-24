import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { TXN_OFFSET_COMMIT_V2 } from './v2';

/*
TxnOffsetCommit Request (Version: 3) => { transactional_id group_id producer_id producer_epoch generation_id member_id group_instance_id (topics) }
  transactional_id => COMPACT_STRING
  group_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  generation_id => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index committed_offset committed_leader_epoch committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING

TxnOffsetCommit Response (Version: 3) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const TXN_OFFSET_COMMIT_V3 = createApi({
    ...TXN_OFFSET_COMMIT_V2,
    apiVersion: 3,
    fallback: TXN_OFFSET_COMMIT_V2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.transactionalId)
            .writeCompactString(data.groupId)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeInt32(data.generationId ?? -1)
            .writeCompactString(data.memberId ?? '')
            .writeCompactString(data.groupInstanceId ?? null)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt64(partition.committedOffset)
                            .writeInt32(partition.committedLeaderEpoch ?? -1)
                            .writeCompactString(partition.committedMetadata)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                name: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
