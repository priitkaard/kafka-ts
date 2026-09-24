import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { END_TXN_V4 } from './v4';

/*
EndTxn Request (Version: 5) => { transactional_id producer_id producer_epoch committed }
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN

EndTxn Response (Version: 5) => { throttle_time_ms error_code producer_id producer_epoch }
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
export const END_TXN_V5 = createApi({
    ...END_TXN_V4,
    apiVersion: 5,
    fallback: END_TXN_V4,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            producerId: decoder.readInt64(),
            producerEpoch: decoder.readInt16(),
            tags: decoder.readTagBuffer(),
        }),
});
