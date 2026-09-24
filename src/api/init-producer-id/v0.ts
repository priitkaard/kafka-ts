import { createApi } from '../../utils/api';
import { InitProducerIdRequest, InitProducerIdResponse, throwIfError } from './common';

/*
InitProducerId Request (Version: 0) => { transactional_id transaction_timeout_ms }
  transactional_id => NULLABLE_STRING
  transaction_timeout_ms => INT32

InitProducerId Response (Version: 0) => { throttle_time_ms error_code producer_id producer_epoch }
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
export const INIT_PRODUCER_ID_V0 = createApi<InitProducerIdRequest, InitProducerIdResponse>({
    apiKey: 22,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeString(data.transactionalId).writeInt32(data.transactionTimeoutMs),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            producerId: decoder.readInt64(),
            producerEpoch: decoder.readInt16(),
            ongoingTxnProducerId: -1n,
            ongoingTxnProducerEpoch: -1,
            tags: {},
        }),
});
