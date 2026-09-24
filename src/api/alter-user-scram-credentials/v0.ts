import { createApi } from '../../utils/api';
import { AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, throwIfError } from './common';

/*
AlterUserScramCredentials Request (Version: 0) => { (deletions) (upsertions) }
  deletions => { name mechanism }
    name => COMPACT_STRING
    mechanism => INT8
  upsertions => { name mechanism iterations salt salted_password }
    name => COMPACT_STRING
    mechanism => INT8
    iterations => INT32
    salt => COMPACT_BYTES
    salted_password => COMPACT_BYTES

AlterUserScramCredentials Response (Version: 0) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { user error_code error_message }
    user => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const ALTER_USER_SCRAM_CREDENTIALS_V0 = createApi<
    AlterUserScramCredentialsRequest,
    AlterUserScramCredentialsResponse
>({
    apiKey: 51,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.deletions, (encoder, deletion) =>
                encoder.writeCompactString(deletion.name).writeInt8(deletion.mechanism).writeTagBuffer(),
            )
            .writeCompactArray(data.upsertions, (encoder, upsertion) =>
                encoder
                    .writeCompactString(upsertion.name)
                    .writeInt8(upsertion.mechanism)
                    .writeInt32(upsertion.iterations)
                    .writeCompactBytes(upsertion.salt)
                    .writeCompactBytes(upsertion.saltedPassword)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readCompactArray((resultItem) => ({
                user: resultItem.readCompactString()!,
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readCompactString(),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
