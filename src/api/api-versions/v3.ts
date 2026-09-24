import { createApi } from '../../utils/api';
import { KafkaTSApiError } from '../../utils/error';
import { UNSUPPORTED_VERSION } from './common';
import { API_VERSIONS_V0 } from './v0';
import { API_VERSIONS_V2 } from './v2';

/*
ApiVersions Request (Version: 3) => { client_software_name client_software_version }
  client_software_name => COMPACT_STRING
  client_software_version => COMPACT_STRING

ApiVersions Response (Version: 3) => { error_code (api_keys) throttle_time_ms supported_features<tag: 0> finalized_features_epoch<tag: 1> finalized_features<tag: 2> zk_migration_ready<tag: 3> }
  error_code => INT16
  api_keys => { api_key min_version max_version }
    api_key => INT16
    min_version => INT16
    max_version => INT16
  throttle_time_ms => INT32
  supported_features<tag: 0> => { name min_version max_version }
    name => COMPACT_STRING
    min_version => INT16
    max_version => INT16
  finalized_features_epoch<tag: 1> => INT64
  finalized_features<tag: 2> => { name max_version_level min_version_level }
    name => COMPACT_STRING
    max_version_level => INT16
    min_version_level => INT16
  zk_migration_ready<tag: 3> => BOOLEAN
*/
export const API_VERSIONS_V3 = createApi({
    ...API_VERSIONS_V2,
    apiVersion: 3,
    fallback: API_VERSIONS_V2,
    requestHeaderVersion: 2,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.clientSoftwareName ?? 'kafka-ts')
            .writeCompactString(data.clientSoftwareVersion ?? 'unknown')
            .writeTagBuffer(),
    response: (decoder) => {
        if (decoder.peekInt16() === UNSUPPORTED_VERSION) return API_VERSIONS_V0.response(decoder);

        const result = {
            errorCode: decoder.readInt16(),
            versions: decoder.readCompactArray((version) => ({
                apiKey: version.readInt16(),
                minVersion: version.readInt16(),
                maxVersion: version.readInt16(),
                tags: version.readTagBuffer(),
            })),
            throttleTimeMs: decoder.readInt32(),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
