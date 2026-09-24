import { createApi } from '../../utils/api';
import { KafkaTSApiError } from '../../utils/error';
import { ApiVersionsRequest, ApiVersionsResponse } from './common';

/*
ApiVersions Request (Version: 0) => { }

ApiVersions Response (Version: 0) => { error_code [api_keys] }
  error_code => INT16
  api_keys => { api_key min_version max_version }
    api_key => INT16
    min_version => INT16
    max_version => INT16
*/
export const API_VERSIONS_V0 = createApi<ApiVersionsRequest, ApiVersionsResponse>({
    apiKey: 18,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder) => encoder,
    response: (decoder) => {
        const result = {
            errorCode: decoder.readInt16(),
            versions: decoder.readArray((version) => ({
                apiKey: version.readInt16(),
                minVersion: version.readInt16(),
                maxVersion: version.readInt16(),
                tags: {},
            })),
            throttleTimeMs: 0,
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
