import { createApi } from '../../utils/api';
import { KafkaTSApiError } from '../../utils/error';
import { SaslHandshakeRequest, SaslHandshakeResponse } from './common';

/*
SaslHandshake Request (Version: 0) => { mechanism }
  mechanism => STRING

SaslHandshake Response (Version: 0) => { error_code [mechanisms] }
  error_code => INT16
  mechanisms => STRING
*/
export const SASL_HANDSHAKE_V0 = createApi<SaslHandshakeRequest, SaslHandshakeResponse>({
    apiKey: 17,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeString(data.mechanism),
    response: (decoder) => {
        const result = {
            errorCode: decoder.readInt16(),
            mechanisms: decoder.readArray((mechanism) => mechanism.readString()!),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
