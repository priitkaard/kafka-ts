import { createApi } from '../../utils/api';
import { UPDATE_FEATURES_V0 } from './v0';

/*
UpdateFeatures Request (Version: 1) => { timeout_ms (feature_updates) validate_only }
  timeout_ms => INT32
  feature_updates => { feature max_version_level upgrade_type }
    feature => COMPACT_STRING
    max_version_level => INT16
    upgrade_type => INT8
  validate_only => BOOLEAN

UpdateFeatures Response (Version: 1) => { throttle_time_ms error_code error_message (results) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  results => { feature error_code error_message }
    feature => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const UPDATE_FEATURES_V1 = createApi({
    ...UPDATE_FEATURES_V0,
    apiVersion: 1,
    fallback: UPDATE_FEATURES_V0,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.timeoutMs)
            .writeCompactArray(data.featureUpdates, (encoder, featureUpdate) =>
                encoder
                    .writeCompactString(featureUpdate.feature)
                    .writeInt16(featureUpdate.maxVersionLevel)
                    .writeInt8(featureUpdate.upgradeType ?? 1)
                    .writeTagBuffer(),
            )
            .writeBoolean(data.validateOnly ?? false)
            .writeTagBuffer(),
});
