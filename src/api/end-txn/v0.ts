import { createApi } from '../../utils/api';
import { EndTxnRequest, EndTxnResponse, throwIfError } from './common';

/*
EndTxn Request (Version: 0) => { transactional_id producer_id producer_epoch committed }
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN

EndTxn Response (Version: 0) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const END_TXN_V0 = createApi<EndTxnRequest, EndTxnResponse>({
    apiKey: 26,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.transactionalId)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeBoolean(data.committed),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            producerId: -1n,
            producerEpoch: -1,
            tags: {},
        }),
});
