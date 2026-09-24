import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { CREATE_DELEGATION_TOKEN_V2 } from './v2';

/*
CreateDelegationToken Request (Version: 3) => { owner_principal_type owner_principal_name (renewers) max_lifetime_ms }
  owner_principal_type => COMPACT_NULLABLE_STRING
  owner_principal_name => COMPACT_NULLABLE_STRING
  renewers => { principal_type principal_name }
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING
  max_lifetime_ms => INT64

CreateDelegationToken Response (Version: 3) => { error_code principal_type principal_name token_requester_principal_type token_requester_principal_name issue_timestamp_ms expiry_timestamp_ms max_timestamp_ms token_id hmac throttle_time_ms }
  error_code => INT16
  principal_type => COMPACT_STRING
  principal_name => COMPACT_STRING
  token_requester_principal_type => COMPACT_STRING
  token_requester_principal_name => COMPACT_STRING
  issue_timestamp_ms => INT64
  expiry_timestamp_ms => INT64
  max_timestamp_ms => INT64
  token_id => COMPACT_STRING
  hmac => COMPACT_BYTES
  throttle_time_ms => INT32
*/
export const CREATE_DELEGATION_TOKEN_V3 = createApi({
    ...CREATE_DELEGATION_TOKEN_V2,
    apiVersion: 3,
    fallback: CREATE_DELEGATION_TOKEN_V2,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.ownerPrincipalType ?? '')
            .writeCompactString(data.ownerPrincipalName ?? '')
            .writeCompactArray(data.renewers, (encoder, renewer) =>
                encoder
                    .writeCompactString(renewer.principalType)
                    .writeCompactString(renewer.principalName)
                    .writeTagBuffer(),
            )
            .writeInt64(data.maxLifetimeMs)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            principalType: decoder.readCompactString()!,
            principalName: decoder.readCompactString()!,
            tokenRequesterPrincipalType: decoder.readCompactString()!,
            tokenRequesterPrincipalName: decoder.readCompactString()!,
            issueTimestampMs: decoder.readInt64(),
            expiryTimestampMs: decoder.readInt64(),
            maxTimestampMs: decoder.readInt64(),
            tokenId: decoder.readCompactString()!,
            hmac: decoder.readCompactBytes()!,
            throttleTimeMs: decoder.readInt32(),
            tags: decoder.readTagBuffer(),
        }),
});
