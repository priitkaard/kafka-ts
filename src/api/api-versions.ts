import { createApi } from '../utils/api.js';
import { KafkaTSApiError } from '../utils/error.js';

type ApiVersionsRequest = {};

type ApiVersionsResponse = {
    errorCode: number;
    versions: {
        apiKey: number;
        minVersion: number;
        maxVersion: number;
    }[];
    throttleTimeMs: number;
};

/*
ApiVersions Request (Version: 2) => 

ApiVersions Response (Version: 2) => error_code [api_keys] throttle_time_ms 
  error_code => INT16
  api_keys => api_key min_version max_version 
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
*/
export const API_VERSIONS = createApi<ApiVersionsRequest, ApiVersionsResponse>({
    apiKey: 18,
    apiVersion: 2,
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
            })),
            throttleTimeMs: decoder.readInt32(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
