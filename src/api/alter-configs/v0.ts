import { createApi } from '../../utils/api';
import { AlterConfigsRequest, AlterConfigsResponse, throwIfError } from './common';

/*
AlterConfigs Request (Version: 0) => { [resources] validate_only }
  resources => { resource_type resource_name [configs] }
    resource_type => INT8
    resource_name => STRING
    configs => { name value }
      name => STRING
      value => NULLABLE_STRING
  validate_only => BOOLEAN

AlterConfigs Response (Version: 0) => { throttle_time_ms [responses] }
  throttle_time_ms => INT32
  responses => { error_code error_message resource_type resource_name }
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
*/
export const ALTER_CONFIGS_V0 = createApi<AlterConfigsRequest, AlterConfigsResponse>({
    apiKey: 33,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.resources, (encoder, resource) =>
                encoder
                    .writeInt8(resource.resourceType)
                    .writeString(resource.resourceName)
                    .writeArray(resource.configs, (encoder, config) =>
                        encoder.writeString(config.name).writeString(config.value),
                    ),
            )
            .writeBoolean(data.validateOnly),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            responses: decoder.readArray((response) => ({
                errorCode: response.readInt16(),
                errorMessage: response.readString(),
                resourceType: response.readInt8(),
                resourceName: response.readString()!,
                tags: {},
            })),
            tags: {},
        }),
});
