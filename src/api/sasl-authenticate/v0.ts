import { createApi } from '../../utils/api';
import { SaslAuthenticateRequest, SaslAuthenticateResponse, throwIfError } from './common';

/*
SaslAuthenticate Request (Version: 0) => { auth_bytes }
  auth_bytes => BYTES

SaslAuthenticate Response (Version: 0) => { error_code error_message auth_bytes }
  error_code => INT16
  error_message => NULLABLE_STRING
  auth_bytes => BYTES
*/
export const SASL_AUTHENTICATE_V0 = createApi<SaslAuthenticateRequest, SaslAuthenticateResponse>({
    apiKey: 36,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeBytes(data.authBytes),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readString(),
            authBytes: decoder.readBytes()!,
            sessionLifetimeMs: 0n,
            tags: {},
        }),
});
