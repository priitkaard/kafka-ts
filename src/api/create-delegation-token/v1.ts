import { createApi } from '../../utils/api';
import { CreateDelegationTokenRequest, CreateDelegationTokenResponse, throwIfError } from './common';

/*
CreateDelegationToken Request (Version: 1) => { [renewers] max_lifetime_ms }
  renewers => { principal_type principal_name }
    principal_type => STRING
    principal_name => STRING
  max_lifetime_ms => INT64

CreateDelegationToken Response (Version: 1) => { error_code principal_type principal_name issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms }
  error_code => INT16
  principal_type => STRING
  principal_name => STRING
  issue_timestamp_ms => INT64
  expiry_timestamp_ms => INT64
  max_timestamp_ms => INT64
  token_id => STRING
  hmac => BYTES
  throttle_time_ms => INT32
*/
export const CREATE_DELEGATION_TOKEN_V1 = createApi<CreateDelegationTokenRequest, CreateDelegationTokenResponse>({
    apiKey: 38,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.renewers, (encoder, renewer) =>
                encoder.writeString(renewer.principalType).writeString(renewer.principalName),
            )
            .writeInt64(data.maxLifetimeMs),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            principalType: decoder.readString()!,
            principalName: decoder.readString()!,
            tokenRequesterPrincipalType: '',
            tokenRequesterPrincipalName: '',
            issueTimestampMs: decoder.readInt64(),
            expiryTimestampMs: decoder.readInt64(),
            maxTimestampMs: decoder.readInt64(),
            tokenId: decoder.readString()!,
            hmac: decoder.readBytes()!,
            throttleTimeMs: decoder.readInt32(),
            tags: {},
        }),
});
