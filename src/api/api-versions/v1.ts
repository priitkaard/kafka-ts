import { createApi } from '../../utils/api';
import { KafkaTSApiError } from '../../utils/error';
import { UNSUPPORTED_VERSION } from './common';
import { API_VERSIONS_V0 } from './v0';

/*
ApiVersions Request (Version: 1) => { }

ApiVersions Response (Version: 1) => { error_code [api_keys] throttle_time_ms }
  error_code => INT16
  api_keys => { api_key min_version max_version }
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
*/
export const API_VERSIONS_V1 = createApi({
    ...API_VERSIONS_V0,
    apiVersion: 1,
    fallback: API_VERSIONS_V0,
    response: (decoder) => {
        if (decoder.peekInt16() === UNSUPPORTED_VERSION) return API_VERSIONS_V0.response(decoder);

        const result = {
            errorCode: decoder.readInt16(),
            versions: decoder.readArray((version) => ({
                apiKey: version.readInt16(),
                minVersion: version.readInt16(),
                maxVersion: version.readInt16(),
                tags: {},
            })),
            throttleTimeMs: decoder.readInt32(),
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
