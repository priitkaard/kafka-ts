import { createApi } from '../../utils/api';
import { ExpireDelegationTokenRequest, ExpireDelegationTokenResponse, throwIfError } from './common';

/*
ExpireDelegationToken Request (Version: 1) => { hmac expiry_time_period_ms }
  hmac => BYTES
  expiry_time_period_ms => INT64

ExpireDelegationToken Response (Version: 1) => { error_code expiry_timestamp_ms throttle_time_ms }
  error_code => INT16
  expiry_timestamp_ms => INT64
  throttle_time_ms => INT32
*/
export const EXPIRE_DELEGATION_TOKEN_V1 = createApi<ExpireDelegationTokenRequest, ExpireDelegationTokenResponse>({
    apiKey: 40,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeBytes(data.hmac).writeInt64(data.expiryTimePeriodMs),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            expiryTimestampMs: decoder.readInt64(),
            throttleTimeMs: decoder.readInt32(),
            tags: {},
        }),
});
