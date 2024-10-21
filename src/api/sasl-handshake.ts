import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const SASL_HANDSHAKE = createApi({
    apiKey: 17,
    apiVersion: 1,
    request: (encoder, data: { mechanism: string }) => encoder.writeString(data.mechanism),
    response: (decoder) => {
        const result = {
            errorCode: decoder.readInt16(),
            mechanisms: decoder.readArray((mechanism) => mechanism.readString()),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
