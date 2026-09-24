import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { INCREMENTAL_ALTER_CONFIGS_V0 } from './v0';

/*
IncrementalAlterConfigs Request (Version: 1) => { (resources) validate_only }
  resources => { resource_type resource_name (configs) }
    resource_type => INT8
    resource_name => COMPACT_STRING
    configs => { name config_operation value }
      name => COMPACT_STRING
      config_operation => INT8
      value => COMPACT_NULLABLE_STRING
  validate_only => BOOLEAN

IncrementalAlterConfigs Response (Version: 1) => { throttle_time_ms (responses) }
  throttle_time_ms => INT32
  responses => { error_code error_message resource_type resource_name }
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    resource_type => INT8
    resource_name => COMPACT_STRING
*/
export const INCREMENTAL_ALTER_CONFIGS_V1 = createApi({
    ...INCREMENTAL_ALTER_CONFIGS_V0,
    apiVersion: 1,
    fallback: INCREMENTAL_ALTER_CONFIGS_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.resources, (encoder, resource) =>
                encoder
                    .writeInt8(resource.resourceType)
                    .writeCompactString(resource.resourceName)
                    .writeCompactArray(resource.configs, (encoder, config) =>
                        encoder
                            .writeCompactString(config.name)
                            .writeInt8(config.configOperation)
                            .writeCompactString(config.value)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeBoolean(data.validateOnly)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readCompactArray((response) => ({
                errorCode: response.readInt16(),
                errorMessage: response.readCompactString(),
                resourceType: response.readInt8(),
                resourceName: response.readCompactString()!,
                tags: response.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
