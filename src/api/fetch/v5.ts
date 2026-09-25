import { createApi } from '../../utils/api';
import { decodeRecordBatch, throwIfError, withDecompressions } from './common';
import { FETCH_V4 } from './v4';

/*
Fetch Request (Version: 5) => { replica_id max_wait_ms min_bytes max_bytes isolation_level [topics] }
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  topics => { topic [partitions] }
    topic => STRING
    partitions => { partition fetch_offset log_start_offset partition_max_bytes }
      partition => INT32
      fetch_offset => INT64
      log_start_offset => INT64
      partition_max_bytes => INT32

Fetch Response (Version: 5) => { throttle_time_ms [responses] }
  throttle_time_ms => INT32
  responses => { topic [partitions] }
    topic => STRING
    partitions => { partition_index error_code high_watermark last_stable_offset log_start_offset ?[aborted_transactions] records }
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => { producer_id first_offset }
        producer_id => INT64
        first_offset => INT64
      records => NULLABLE_RECORDS
*/
export const FETCH_V5 = createApi({
    ...FETCH_V4,
    apiVersion: 5,
    fallback: FETCH_V4,
    request: (encoder, data) =>
        encoder
            .writeInt32(-1)
            .writeInt32(data.maxWaitMs)
            .writeInt32(data.minBytes)
            .writeInt32(data.maxBytes)
            .writeInt8(data.isolationLevel)
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.topicName)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partition)
                            .writeInt64(partition.fetchOffset)
                            .writeInt64(partition.logStartOffset)
                            .writeInt32(partition.partitionMaxBytes),
                    ),
            ),
    response: async (decoder) =>
        throwIfError(
            await withDecompressions((decompressions) => ({
                throttleTimeMs: decoder.readInt32(),
                errorCode: 0,
                sessionId: 0,
                responses: decoder.readArray((response) => ({
                    topicName: response.readString()!,
                    partitions: response.readArray((partition) => ({
                        partitionIndex: partition.readInt32(),
                        errorCode: partition.readInt16(),
                        highWatermark: partition.readInt64(),
                        lastStableOffset: partition.readInt64(),
                        logStartOffset: partition.readInt64(),
                        abortedTransactions: partition.readArray((abortedTransaction) => ({
                            producerId: abortedTransaction.readInt64(),
                            firstOffset: abortedTransaction.readInt64(),
                        })),
                        preferredReadReplica: -1,
                        records: decodeRecordBatch(partition, partition.readInt32(), decompressions),
                    })),
                })),
            })),
        ),
});
