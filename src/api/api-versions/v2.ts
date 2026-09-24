import { createApi } from '../../utils/api';
import { API_VERSIONS_V1 } from './v1';

/*
ApiVersions Request (Version: 2) => { }

ApiVersions Response (Version: 2) => { error_code [api_keys] throttle_time_ms }
  error_code => INT16
  api_keys => { api_key min_version max_version }
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
*/
export const API_VERSIONS_V2 = createApi({ ...API_VERSIONS_V1, apiVersion: 2, fallback: API_VERSIONS_V1 });
