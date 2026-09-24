import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { ADD_PARTITIONS_TO_TXN_V3 } from './v3';

/*
AddPartitionsToTxn Request (Version: 4) => { (transactions) }
  transactions => { transactional_id producer_id producer_epoch verify_only (topics) }
    transactional_id => COMPACT_STRING
    producer_id => INT64
    producer_epoch => INT16
    verify_only => BOOLEAN
    topics => { name (partitions) }
      name => COMPACT_STRING
      partitions => INT32

AddPartitionsToTxn Response (Version: 4) => { throttle_time_ms error_code (results_by_transaction) }
  throttle_time_ms => INT32
  error_code => INT16
  results_by_transaction => { transactional_id (topic_results) }
    transactional_id => COMPACT_STRING
    topic_results => { name (results_by_partition) }
      name => COMPACT_STRING
      results_by_partition => { partition_index partition_error_code }
        partition_index => INT32
        partition_error_code => INT16
*/
export const ADD_PARTITIONS_TO_TXN_V4 = createApi({
    ...ADD_PARTITIONS_TO_TXN_V3,
    apiVersion: 4,
    fallback: ADD_PARTITIONS_TO_TXN_V3,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.transactions ?? [], (encoder, transaction) =>
                encoder
                    .writeCompactString(transaction.transactionalId)
                    .writeInt64(transaction.producerId)
                    .writeInt16(transaction.producerEpoch)
                    .writeBoolean(transaction.verifyOnly)
                    .writeCompactArray(transaction.topics, (encoder, topic) =>
                        encoder
                            .writeCompactString(topic.name)
                            .writeCompactArray(topic.partitions, (encoder, partition) => encoder.writeInt32(partition))
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            resultsByTopicV3AndBelow: [],
            errorCode: decoder.readInt16(),
            resultsByTransaction: decoder.readCompactArray((resultsByTransaction) => ({
                transactionalId: resultsByTransaction.readCompactString()!,
                topicResults: resultsByTransaction.readCompactArray((topicResult) => ({
                    name: topicResult.readCompactString()!,
                    resultsByPartition: topicResult.readCompactArray((resultsByPartition) => ({
                        partitionIndex: resultsByPartition.readInt32(),
                        partitionErrorCode: resultsByPartition.readInt16(),
                        tags: resultsByPartition.readTagBuffer(),
                    })),
                    tags: topicResult.readTagBuffer(),
                })),
                tags: resultsByTransaction.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
