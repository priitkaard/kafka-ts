import { createApi } from '../../utils/api';
import { LIST_TRANSACTIONS_V1 } from './v1';

/*
ListTransactions Request (Version: 2) => { (state_filters) (producer_id_filters) duration_filter transactional_id_pattern }
  state_filters => COMPACT_STRING
  producer_id_filters => INT64
  duration_filter => INT64
  transactional_id_pattern => COMPACT_NULLABLE_STRING

ListTransactions Response (Version: 2) => { throttle_time_ms error_code (unknown_state_filters) (transaction_states) }
  throttle_time_ms => INT32
  error_code => INT16
  unknown_state_filters => COMPACT_STRING
  transaction_states => { transactional_id producer_id transaction_state }
    transactional_id => COMPACT_STRING
    producer_id => INT64
    transaction_state => COMPACT_STRING
*/
export const LIST_TRANSACTIONS_V2 = createApi({
    ...LIST_TRANSACTIONS_V1,
    apiVersion: 2,
    fallback: LIST_TRANSACTIONS_V1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.stateFilters, (encoder, stateFilter) => encoder.writeCompactString(stateFilter))
            .writeCompactArray(data.producerIdFilters, (encoder, producerIdFilter) =>
                encoder.writeInt64(producerIdFilter),
            )
            .writeInt64(data.durationFilter ?? -1n)
            .writeCompactString(data.transactionalIdPattern ?? null)
            .writeTagBuffer(),
});
