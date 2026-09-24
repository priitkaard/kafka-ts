import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_DELEGATION_TOKEN_V1 } from './v1';

/*
DescribeDelegationToken Request (Version: 2) => { ?(owners) }
  owners => { principal_type principal_name }
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING

DescribeDelegationToken Response (Version: 2) => { error_code (tokens) throttle_time_ms }
  error_code => INT16
  tokens => { principal_type principal_name issue_timestamp expiry_timestamp max_timestamp token_id hmac (renewers) }
    principal_type => COMPACT_STRING
    principal_name => COMPACT_STRING
    issue_timestamp => INT64
    expiry_timestamp => INT64
    max_timestamp => INT64
    token_id => COMPACT_STRING
    hmac => COMPACT_BYTES
    renewers => { principal_type principal_name }
      principal_type => COMPACT_STRING
      principal_name => COMPACT_STRING
  throttle_time_ms => INT32
*/
export const DESCRIBE_DELEGATION_TOKEN_V2 = createApi({
    ...DESCRIBE_DELEGATION_TOKEN_V1,
    apiVersion: 2,
    fallback: DESCRIBE_DELEGATION_TOKEN_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.owners, (encoder, owner) =>
                encoder
                    .writeCompactString(owner.principalType)
                    .writeCompactString(owner.principalName)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            tokens: decoder.readCompactArray((token) => ({
                principalType: token.readCompactString()!,
                principalName: token.readCompactString()!,
                tokenRequesterPrincipalType: '',
                tokenRequesterPrincipalName: '',
                issueTimestamp: token.readInt64(),
                expiryTimestamp: token.readInt64(),
                maxTimestamp: token.readInt64(),
                tokenId: token.readCompactString()!,
                hmac: token.readCompactBytes()!,
                renewers: token.readCompactArray((renewer) => ({
                    principalType: renewer.readCompactString()!,
                    principalName: renewer.readCompactString()!,
                    tags: renewer.readTagBuffer(),
                })),
                tags: token.readTagBuffer(),
            })),
            throttleTimeMs: decoder.readInt32(),
            tags: decoder.readTagBuffer(),
        }),
});
