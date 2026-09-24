import { createApi } from '../../utils/api';
import { throwIfError, UpdateFeaturesRequest, UpdateFeaturesResponse } from './common';

/*
UpdateFeatures Request (Version: 0) => { timeout_ms (feature_updates) }
  timeout_ms => INT32
  feature_updates => { feature max_version_level allow_downgrade }
    feature => COMPACT_STRING
    max_version_level => INT16
    allow_downgrade => BOOLEAN

UpdateFeatures Response (Version: 0) => { throttle_time_ms error_code error_message (results) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  results => { feature error_code error_message }
    feature => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const UPDATE_FEATURES_V0 = createApi<UpdateFeaturesRequest, UpdateFeaturesResponse>({
    apiKey: 57,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.timeoutMs)
            .writeCompactArray(data.featureUpdates, (encoder, featureUpdate) =>
                encoder
                    .writeCompactString(featureUpdate.feature)
                    .writeInt16(featureUpdate.maxVersionLevel)
                    .writeBoolean(featureUpdate.allowDowngrade ?? false)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            results: decoder.readCompactArray((resultItem) => ({
                feature: resultItem.readCompactString()!,
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readCompactString(),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
