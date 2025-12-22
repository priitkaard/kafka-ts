import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type SaslHandshakeRequest = {
    mechanism: string;
};

type SaslHandshakeResponse = {
    errorCode: number;
    mechanisms: string[];
};

/*
SaslHandshake Request (Version: 1) => mechanism 
  mechanism => STRING

SaslHandshake Response (Version: 1) => error_code [mechanisms] 
  error_code => INT16
  mechanisms => STRING
*/
export const SASL_HANDSHAKE = createApi<SaslHandshakeRequest, SaslHandshakeResponse>({
    apiKey: 17,
    apiVersion: 1,
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
