import { createApi } from '../../utils/api';
import { decodeRecordBatch, throwIfError } from './common';
import { FETCH_V12 } from './v12';

/*
Fetch Request (Version: 13) => { replica_id max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch (topics) (forgotten_topics_data) rack_id cluster_id<tag: 0> }
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes }
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => { topic_id (partitions) }
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING
  cluster_id<tag: 0> => COMPACT_NULLABLE_STRING

Fetch Response (Version: 13) => { throttle_time_ms error_code session_id (responses) }
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index error_code high_watermark last_stable_offset log_start_offset ?(aborted_transactions) preferred_read_replica records diverging_epoch<tag: 0> current_leader<tag: 1> snapshot_id<tag: 2> }
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => { producer_id first_offset }
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_NULLABLE_RECORDS
      diverging_epoch<tag: 0> => { epoch end_offset }
        epoch => INT32
        end_offset => INT64
      current_leader<tag: 1> => { leader_id leader_epoch }
        leader_id => INT32
        leader_epoch => INT32
      snapshot_id<tag: 2> => { end_offset epoch }
        end_offset => INT64
        epoch => INT32
*/
export const FETCH_V13 = createApi({
    ...FETCH_V12,
    apiVersion: 13,
    fallback: FETCH_V12,
    request: (encoder, data) =>
        encoder
            .writeInt32(-1)
            .writeInt32(data.maxWaitMs)
            .writeInt32(data.minBytes)
            .writeInt32(data.maxBytes)
            .writeInt8(data.isolationLevel)
            .writeInt32(data.sessionId)
            .writeInt32(data.sessionEpoch)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeUUID(topic.topicId)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partition)
                            .writeInt32(partition.currentLeaderEpoch)
                            .writeInt64(partition.fetchOffset)
                            .writeInt32(partition.lastFetchedEpoch)
                            .writeInt64(partition.logStartOffset)
                            .writeInt32(partition.partitionMaxBytes)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeCompactArray(data.forgottenTopicsData, (encoder, forgottenTopic) =>
                encoder
                    .writeUUID(forgottenTopic.topicId)
                    .writeCompactArray(forgottenTopic.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeTagBuffer(),
            )
            .writeCompactString(data.rackId)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            sessionId: decoder.readInt32(),
            responses: decoder.readCompactArray((response) => ({
                topicId: response.readUUID(),
                partitions: response.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    highWatermark: partition.readInt64(),
                    lastStableOffset: partition.readInt64(),
                    logStartOffset: partition.readInt64(),
                    abortedTransactions: partition.readCompactArray((abortedTransaction) => ({
                        producerId: abortedTransaction.readInt64(),
                        firstOffset: abortedTransaction.readInt64(),
                        tags: abortedTransaction.readTagBuffer(),
                    })),
                    preferredReadReplica: partition.readInt32(),
                    records: decodeRecordBatch(partition, partition.readUVarInt() - 1),
                    tags: partition.readTagBuffer(),
                })),
                tags: response.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
