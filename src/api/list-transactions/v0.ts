import { createApi } from '../../utils/api';
import { ListTransactionsRequest, ListTransactionsResponse, throwIfError } from './common';

/*
ListTransactions Request (Version: 0) => { (state_filters) (producer_id_filters) }
  state_filters => COMPACT_STRING
  producer_id_filters => INT64

ListTransactions Response (Version: 0) => { throttle_time_ms error_code (unknown_state_filters) (transaction_states) }
  throttle_time_ms => INT32
  error_code => INT16
  unknown_state_filters => COMPACT_STRING
  transaction_states => { transactional_id producer_id transaction_state }
    transactional_id => COMPACT_STRING
    producer_id => INT64
    transaction_state => COMPACT_STRING
*/
export const LIST_TRANSACTIONS_V0 = createApi<ListTransactionsRequest, ListTransactionsResponse>({
    apiKey: 66,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.stateFilters, (encoder, stateFilter) => encoder.writeCompactString(stateFilter))
            .writeCompactArray(data.producerIdFilters, (encoder, producerIdFilter) =>
                encoder.writeInt64(producerIdFilter),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            unknownStateFilters: decoder.readCompactArray((unknownStateFilter) =>
                unknownStateFilter.readCompactString()!,
            ),
            transactionStates: decoder.readCompactArray((transactionState) => ({
                transactionalId: transactionState.readCompactString()!,
                producerId: transactionState.readInt64(),
                transactionState: transactionState.readCompactString()!,
                tags: transactionState.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
