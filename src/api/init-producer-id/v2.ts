import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { INIT_PRODUCER_ID_V1 } from './v1';

/*
InitProducerId Request (Version: 2) => { transactional_id transaction_timeout_ms }
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32

InitProducerId Response (Version: 2) => { throttle_time_ms error_code producer_id producer_epoch }
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
export const INIT_PRODUCER_ID_V2 = createApi({
    ...INIT_PRODUCER_ID_V1,
    apiVersion: 2,
    fallback: INIT_PRODUCER_ID_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder.writeCompactString(data.transactionalId).writeInt32(data.transactionTimeoutMs).writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            producerId: decoder.readInt64(),
            producerEpoch: decoder.readInt16(),
            ongoingTxnProducerId: -1n,
            ongoingTxnProducerEpoch: -1,
            tags: decoder.readTagBuffer(),
        }),
});
