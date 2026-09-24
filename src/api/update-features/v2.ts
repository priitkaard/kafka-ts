import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { UPDATE_FEATURES_V1 } from './v1';

/*
UpdateFeatures Request (Version: 2) => { timeout_ms (feature_updates) validate_only }
  timeout_ms => INT32
  feature_updates => { feature max_version_level upgrade_type }
    feature => COMPACT_STRING
    max_version_level => INT16
    upgrade_type => INT8
  validate_only => BOOLEAN

UpdateFeatures Response (Version: 2) => { throttle_time_ms error_code error_message }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
*/
export const UPDATE_FEATURES_V2 = createApi({
    ...UPDATE_FEATURES_V1,
    apiVersion: 2,
    fallback: UPDATE_FEATURES_V1,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            results: [],
            tags: decoder.readTagBuffer(),
        }),
});
