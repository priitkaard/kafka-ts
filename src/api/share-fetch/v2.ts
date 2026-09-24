import { createApi } from '../../utils/api';
import { SHARE_FETCH_V1 } from './v1';

/*
ShareFetch Request (Version: 2) => { group_id member_id share_session_epoch max_wait_ms min_bytes max_bytes max_records batch_size share_acquire_mode is_renew_ack (topics) (forgotten_topics_data) }
  group_id => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  share_session_epoch => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  max_records => INT32
  batch_size => INT32
  share_acquire_mode => INT8
  is_renew_ack => BOOLEAN
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index (acknowledgement_batches) }
      partition_index => INT32
      acknowledgement_batches => { first_offset last_offset (acknowledge_types) }
        first_offset => INT64
        last_offset => INT64
        acknowledge_types => INT8
  forgotten_topics_data => { topic_id (partitions) }
    topic_id => UUID
    partitions => INT32

ShareFetch Response (Version: 2) => { throttle_time_ms error_code error_message acquisition_lock_timeout_ms (responses) (node_endpoints) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  acquisition_lock_timeout_ms => INT32
  responses => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index error_code error_message acknowledge_error_code acknowledge_error_message current_leader records (acquired_records) }
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      acknowledge_error_code => INT16
      acknowledge_error_message => COMPACT_NULLABLE_STRING
      current_leader => { leader_id leader_epoch }
        leader_id => INT32
        leader_epoch => INT32
      records => COMPACT_RECORDS
      acquired_records => { first_offset last_offset delivery_count }
        first_offset => INT64
        last_offset => INT64
        delivery_count => INT16
  node_endpoints => { node_id host port rack }
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
*/
export const SHARE_FETCH_V2 = createApi({
    ...SHARE_FETCH_V1,
    apiVersion: 2,
    fallback: SHARE_FETCH_V1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactString(data.memberId)
            .writeInt32(data.shareSessionEpoch)
            .writeInt32(data.maxWaitMs)
            .writeInt32(data.minBytes)
            .writeInt32(data.maxBytes)
            .writeInt32(data.maxRecords)
            .writeInt32(data.batchSize)
            .writeInt8(data.shareAcquireMode ?? 0)
            .writeBoolean(data.isRenewAck ?? false)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeUUID(topic.topicId)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeCompactArray(partition.acknowledgementBatches, (encoder, acknowledgementBatche) =>
                                encoder
                                    .writeInt64(acknowledgementBatche.firstOffset)
                                    .writeInt64(acknowledgementBatche.lastOffset)
                                    .writeCompactArray(
                                        acknowledgementBatche.acknowledgeTypes,
                                        (encoder, acknowledgeType) => encoder.writeInt8(acknowledgeType),
                                    )
                                    .writeTagBuffer(),
                            )
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeCompactArray(data.forgottenTopicsData, (encoder, forgottenTopicsData) =>
                encoder
                    .writeUUID(forgottenTopicsData.topicId)
                    .writeCompactArray(forgottenTopicsData.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
});
