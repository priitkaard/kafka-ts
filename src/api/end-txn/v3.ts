import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { END_TXN_V2 } from './v2';

/*
EndTxn Request (Version: 3) => { transactional_id producer_id producer_epoch committed }
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN

EndTxn Response (Version: 3) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const END_TXN_V3 = createApi({
    ...END_TXN_V2,
    apiVersion: 3,
    fallback: END_TXN_V2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.transactionalId)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeBoolean(data.committed)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            producerId: -1n,
            producerEpoch: -1,
            tags: decoder.readTagBuffer(),
        }),
});
