import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { RENEW_DELEGATION_TOKEN_V1 } from './v1';

/*
RenewDelegationToken Request (Version: 2) => { hmac renew_period_ms }
  hmac => COMPACT_BYTES
  renew_period_ms => INT64

RenewDelegationToken Response (Version: 2) => { error_code expiry_timestamp_ms throttle_time_ms }
  error_code => INT16
  expiry_timestamp_ms => INT64
  throttle_time_ms => INT32
*/
export const RENEW_DELEGATION_TOKEN_V2 = createApi({
    ...RENEW_DELEGATION_TOKEN_V1,
    apiVersion: 2,
    fallback: RENEW_DELEGATION_TOKEN_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) => encoder.writeCompactBytes(data.hmac).writeInt64(data.renewPeriodMs).writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            expiryTimestampMs: decoder.readInt64(),
            throttleTimeMs: decoder.readInt32(),
            tags: decoder.readTagBuffer(),
        }),
});
