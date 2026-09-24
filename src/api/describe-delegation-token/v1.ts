import { createApi } from '../../utils/api';
import { DescribeDelegationTokenRequest, DescribeDelegationTokenResponse, throwIfError } from './common';

/*
DescribeDelegationToken Request (Version: 1) => { ?[owners] }
  owners => { principal_type principal_name }
    principal_type => STRING
    principal_name => STRING

DescribeDelegationToken Response (Version: 1) => { error_code [tokens] throttle_time_ms }
  error_code => INT16
  tokens => { principal_type principal_name issue_timestamp expiry_timestamp max_timestamp token_id hmac [renewers] }
    principal_type => STRING
    principal_name => STRING
    issue_timestamp => INT64
    expiry_timestamp => INT64
    max_timestamp => INT64
    token_id => STRING
    hmac => BYTES
    renewers => { principal_type principal_name }
      principal_type => STRING
      principal_name => STRING
  throttle_time_ms => INT32
*/
export const DESCRIBE_DELEGATION_TOKEN_V1 = createApi<DescribeDelegationTokenRequest, DescribeDelegationTokenResponse>({
    apiKey: 41,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.owners, (encoder, owner) =>
            encoder.writeString(owner.principalType).writeString(owner.principalName),
        ),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            tokens: decoder.readArray((token) => ({
                principalType: token.readString()!,
                principalName: token.readString()!,
                tokenRequesterPrincipalType: '',
                tokenRequesterPrincipalName: '',
                issueTimestamp: token.readInt64(),
                expiryTimestamp: token.readInt64(),
                maxTimestamp: token.readInt64(),
                tokenId: token.readString()!,
                hmac: token.readBytes()!,
                renewers: token.readArray((renewer) => ({
                    principalType: renewer.readString()!,
                    principalName: renewer.readString()!,
                    tags: {},
                })),
                tags: {},
            })),
            throttleTimeMs: decoder.readInt32(),
            tags: {},
        }),
});
