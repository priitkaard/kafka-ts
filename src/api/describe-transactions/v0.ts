import { createApi } from '../../utils/api';
import { DescribeTransactionsRequest, DescribeTransactionsResponse, throwIfError } from './common';

/*
DescribeTransactions Request (Version: 0) => { (transactional_ids) }
  transactional_ids => COMPACT_STRING

DescribeTransactions Response (Version: 0) => { throttle_time_ms (transaction_states) }
  throttle_time_ms => INT32
  transaction_states => { error_code transactional_id transaction_state transaction_timeout_ms transaction_start_time_ms producer_id producer_epoch (topics) }
    error_code => INT16
    transactional_id => COMPACT_STRING
    transaction_state => COMPACT_STRING
    transaction_timeout_ms => INT32
    transaction_start_time_ms => INT64
    producer_id => INT64
    producer_epoch => INT16
    topics => { topic (partitions) }
      topic => COMPACT_STRING
      partitions => INT32
*/
export const DESCRIBE_TRANSACTIONS_V0 = createApi<DescribeTransactionsRequest, DescribeTransactionsResponse>({
    apiKey: 65,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.transactionalIds, (encoder, transactionalId) =>
                encoder.writeCompactString(transactionalId),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            transactionStates: decoder.readCompactArray((transactionState) => ({
                errorCode: transactionState.readInt16(),
                transactionalId: transactionState.readCompactString()!,
                transactionState: transactionState.readCompactString()!,
                transactionTimeoutMs: transactionState.readInt32(),
                transactionStartTimeMs: transactionState.readInt64(),
                producerId: transactionState.readInt64(),
                producerEpoch: transactionState.readInt16(),
                topics: transactionState.readCompactArray((topic) => ({
                    topic: topic.readCompactString()!,
                    partitions: topic.readCompactArray((partition) => partition.readInt32()),
                    tags: topic.readTagBuffer(),
                })),
                tags: transactionState.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
