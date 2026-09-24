import { createApi } from '../../utils/api';
import { RenewDelegationTokenRequest, RenewDelegationTokenResponse, throwIfError } from './common';

/*
RenewDelegationToken Request (Version: 1) => { hmac renew_period_ms }
  hmac => BYTES
  renew_period_ms => INT64

RenewDelegationToken Response (Version: 1) => { error_code expiry_timestamp_ms throttle_time_ms }
  error_code => INT16
  expiry_timestamp_ms => INT64
  throttle_time_ms => INT32
*/
export const RENEW_DELEGATION_TOKEN_V1 = createApi<RenewDelegationTokenRequest, RenewDelegationTokenResponse>({
    apiKey: 39,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeBytes(data.hmac).writeInt64(data.renewPeriodMs),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            expiryTimestampMs: decoder.readInt64(),
            throttleTimeMs: decoder.readInt32(),
            tags: {},
        }),
});
