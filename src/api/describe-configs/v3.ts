import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_CONFIGS_V2 } from './v2';

/*
DescribeConfigs Request (Version: 3) => { [resources] include_synonyms include_documentation }
  resources => { resource_type resource_name ?[configuration_keys] }
    resource_type => INT8
    resource_name => STRING
    configuration_keys => STRING
  include_synonyms => BOOLEAN
  include_documentation => BOOLEAN

DescribeConfigs Response (Version: 3) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { error_code error_message resource_type resource_name [configs] }
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
    configs => { name value read_only config_source is_sensitive [synonyms] config_type documentation }
      name => STRING
      value => NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
      synonyms => { name value source }
        name => STRING
        value => NULLABLE_STRING
        source => INT8
      config_type => INT8
      documentation => NULLABLE_STRING
*/
export const DESCRIBE_CONFIGS_V3 = createApi({
    ...DESCRIBE_CONFIGS_V2,
    apiVersion: 3,
    fallback: DESCRIBE_CONFIGS_V2,
    request: (encoder, data) =>
        encoder
            .writeArray(data.resources, (encoder, resource) =>
                encoder
                    .writeInt8(resource.resourceType)
                    .writeString(resource.resourceName)
                    .writeArray(resource.configurationKeys, (encoder, configurationKey) =>
                        encoder.writeString(configurationKey),
                    ),
            )
            .writeBoolean(data.includeSynonyms)
            .writeBoolean(data.includeDocumentation ?? false),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readArray((resultItem) => ({
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readString(),
                resourceType: resultItem.readInt8(),
                resourceName: resultItem.readString()!,
                configs: resultItem.readArray((config) => ({
                    name: config.readString()!,
                    value: config.readString(),
                    readOnly: config.readBoolean(),
                    configSource: config.readInt8(),
                    isSensitive: config.readBoolean(),
                    synonyms: config.readArray((synonym) => ({
                        name: synonym.readString()!,
                        value: synonym.readString(),
                        source: synonym.readInt8(),
                        tags: {},
                    })),
                    configType: config.readInt8(),
                    documentation: config.readString(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
