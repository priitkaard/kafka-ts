import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { SASL_AUTHENTICATE_V0 } from './v0';

/*
SaslAuthenticate Request (Version: 1) => { auth_bytes }
  auth_bytes => BYTES

SaslAuthenticate Response (Version: 1) => { error_code error_message auth_bytes session_lifetime_ms }
  error_code => INT16
  error_message => NULLABLE_STRING
  auth_bytes => BYTES
  session_lifetime_ms => INT64
*/
export const SASL_AUTHENTICATE_V1 = createApi({
    ...SASL_AUTHENTICATE_V0,
    apiVersion: 1,
    fallback: SASL_AUTHENTICATE_V0,
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readString(),
            authBytes: decoder.readBytes()!,
            sessionLifetimeMs: decoder.readInt64(),
            tags: {},
        }),
});
