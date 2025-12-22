import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type SaslAuthenticateRequest = {
    authBytes: Buffer;
};

type SaslAuthenticateResponse = {
    errorCode: number;
    errorMessage: string | null;
    authBytes: Buffer;
    sessionLifetimeMs: bigint;
    tags: Record<number, Buffer>;
};

/*
SaslAuthenticate Request (Version: 0) => auth_bytes 
  auth_bytes => BYTES

SaslAuthenticate Response (Version: 0) => error_code error_message auth_bytes 
  error_code => INT16
  error_message => NULLABLE_STRING
  auth_bytes => BYTES
*/
const SASL_AUTHENTICATE_V0 = createApi<SaslAuthenticateRequest, SaslAuthenticateResponse>({
    apiKey: 36,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeBytes(data.authBytes),
    response: (decoder) => {
        const result = {
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readString(),
            authBytes: decoder.readBytes()!,
            sessionLifetimeMs: BigInt(0),
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
        return result;
    },
});

/*
SaslAuthenticate Request (Version: 2) => auth_bytes _tagged_fields 
  auth_bytes => COMPACT_BYTES

SaslAuthenticate Response (Version: 2) => error_code error_message auth_bytes session_lifetime_ms _tagged_fields 
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  auth_bytes => COMPACT_BYTES
  session_lifetime_ms => INT64
*/
export const SASL_AUTHENTICATE = createApi<SaslAuthenticateRequest, SaslAuthenticateResponse>({
    apiKey: 36,
    apiVersion: 2,
    fallback: SASL_AUTHENTICATE_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data: { authBytes: Buffer }) => encoder.writeCompactBytes(data.authBytes).writeTagBuffer(),
    response: (decoder) => {
        const result = {
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            authBytes: decoder.readCompactBytes()!,
            sessionLifetimeMs: decoder.readInt64(),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
        return result;
    },
});
