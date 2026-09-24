import { createApi } from '../../utils/api';
import { DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse, throwIfError } from './common';

/*
DescribeUserScramCredentials Request (Version: 0) => { ?(users) }
  users => { name }
    name => COMPACT_STRING

DescribeUserScramCredentials Response (Version: 0) => { throttle_time_ms error_code error_message (results) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  results => { user error_code error_message (credential_infos) }
    user => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    credential_infos => { mechanism iterations }
      mechanism => INT8
      iterations => INT32
*/
export const DESCRIBE_USER_SCRAM_CREDENTIALS_V0 = createApi<
    DescribeUserScramCredentialsRequest,
    DescribeUserScramCredentialsResponse
>({
    apiKey: 50,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.users, (encoder, user) => encoder.writeCompactString(user.name).writeTagBuffer())
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            results: decoder.readCompactArray((resultItem) => ({
                user: resultItem.readCompactString()!,
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readCompactString(),
                credentialInfos: resultItem.readCompactArray((credentialInfo) => ({
                    mechanism: credentialInfo.readInt8(),
                    iterations: credentialInfo.readInt32(),
                    tags: credentialInfo.readTagBuffer(),
                })),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
