import { createApi } from '../../utils/api';
import { INIT_PRODUCER_ID_V2 } from './v2';

/*
InitProducerId Request (Version: 3) => { transactional_id transaction_timeout_ms producer_id producer_epoch }
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16

InitProducerId Response (Version: 3) => { throttle_time_ms error_code producer_id producer_epoch }
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
export const INIT_PRODUCER_ID_V3 = createApi({
    ...INIT_PRODUCER_ID_V2,
    apiVersion: 3,
    fallback: INIT_PRODUCER_ID_V2,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.transactionalId)
            .writeInt32(data.transactionTimeoutMs)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeTagBuffer(),
});
