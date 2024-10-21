import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const SASL_AUTHENTICATE = createApi({
    apiKey: 36,
    apiVersion: 2,
    request: (encoder, data: { authBytes: Buffer }) =>
        encoder.writeUVarInt(0).writeCompactBytes(data.authBytes).writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            authBytes: decoder.readCompactBytes(),
            sessionLifetimeMs: decoder.readInt64(),
            _tag2: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
        return result;
    },
});
