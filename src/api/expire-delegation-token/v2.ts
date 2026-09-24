import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { EXPIRE_DELEGATION_TOKEN_V1 } from './v1';

/*
ExpireDelegationToken Request (Version: 2) => { hmac expiry_time_period_ms }
  hmac => COMPACT_BYTES
  expiry_time_period_ms => INT64

ExpireDelegationToken Response (Version: 2) => { error_code expiry_timestamp_ms throttle_time_ms }
  error_code => INT16
  expiry_timestamp_ms => INT64
  throttle_time_ms => INT32
*/
export const EXPIRE_DELEGATION_TOKEN_V2 = createApi({
    ...EXPIRE_DELEGATION_TOKEN_V1,
    apiVersion: 2,
    fallback: EXPIRE_DELEGATION_TOKEN_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder.writeCompactBytes(data.hmac).writeInt64(data.expiryTimePeriodMs).writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            expiryTimestampMs: decoder.readInt64(),
            throttleTimeMs: decoder.readInt32(),
            tags: decoder.readTagBuffer(),
        }),
});
