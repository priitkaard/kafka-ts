import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const SASL_AUTHENTICATE = createApi({
    apiKey: 36,
    apiVersion: 2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data: { authBytes: Buffer }) => encoder.writeCompactBytes(data.authBytes).writeTagBuffer(),
    response: (decoder) => {
        const result = {
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            authBytes: decoder.readCompactBytes(),
            sessionLifetimeMs: decoder.readInt64(),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
        return result;
    },
});
