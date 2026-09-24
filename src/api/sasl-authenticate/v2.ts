import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { SASL_AUTHENTICATE_V1 } from './v1';

/*
SaslAuthenticate Request (Version: 2) => { auth_bytes }
  auth_bytes => COMPACT_BYTES

SaslAuthenticate Response (Version: 2) => { error_code error_message auth_bytes session_lifetime_ms }
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  auth_bytes => COMPACT_BYTES
  session_lifetime_ms => INT64
*/
export const SASL_AUTHENTICATE_V2 = createApi({
    ...SASL_AUTHENTICATE_V1,
    apiVersion: 2,
    fallback: SASL_AUTHENTICATE_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) => encoder.writeCompactBytes(data.authBytes).writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            authBytes: decoder.readCompactBytes()!,
            sessionLifetimeMs: decoder.readInt64(),
            tags: decoder.readTagBuffer(),
        }),
});
