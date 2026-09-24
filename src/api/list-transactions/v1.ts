import { createApi } from '../../utils/api';
import { LIST_TRANSACTIONS_V0 } from './v0';

/*
ListTransactions Request (Version: 1) => { (state_filters) (producer_id_filters) duration_filter }
  state_filters => COMPACT_STRING
  producer_id_filters => INT64
  duration_filter => INT64

ListTransactions Response (Version: 1) => { throttle_time_ms error_code (unknown_state_filters) (transaction_states) }
  throttle_time_ms => INT32
  error_code => INT16
  unknown_state_filters => COMPACT_STRING
  transaction_states => { transactional_id producer_id transaction_state }
    transactional_id => COMPACT_STRING
    producer_id => INT64
    transaction_state => COMPACT_STRING
*/
export const LIST_TRANSACTIONS_V1 = createApi({
    ...LIST_TRANSACTIONS_V0,
    apiVersion: 1,
    fallback: LIST_TRANSACTIONS_V0,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.stateFilters, (encoder, stateFilter) => encoder.writeCompactString(stateFilter))
            .writeCompactArray(data.producerIdFilters, (encoder, producerIdFilter) =>
                encoder.writeInt64(producerIdFilter),
            )
            .writeInt64(data.durationFilter ?? -1n)
            .writeTagBuffer(),
});
