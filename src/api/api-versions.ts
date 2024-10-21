import { createApi } from '../utils/api.js';
import { KafkaTSApiError } from '../utils/error.js';

export const API_VERSIONS = createApi({
    apiKey: 18,
    apiVersion: 2,
    request: (encoder) => encoder,
    response: (decoder) => {
        const result = {
            errorCode: decoder.readInt16(),
            versions: decoder.readArray((version) => ({
                apiKey: version.readInt16(),
                minVersion: version.readInt16(),
                maxVersion: version.readInt16(),
            })),
            throttleTimeMs: decoder.readInt32(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
