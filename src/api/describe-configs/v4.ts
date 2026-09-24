import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_CONFIGS_V3 } from './v3';

/*
DescribeConfigs Request (Version: 4) => { (resources) include_synonyms include_documentation }
  resources => { resource_type resource_name ?(configuration_keys) }
    resource_type => INT8
    resource_name => COMPACT_STRING
    configuration_keys => COMPACT_STRING
  include_synonyms => BOOLEAN
  include_documentation => BOOLEAN

DescribeConfigs Response (Version: 4) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { error_code error_message resource_type resource_name (configs) }
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    resource_type => INT8
    resource_name => COMPACT_STRING
    configs => { name value read_only config_source is_sensitive (synonyms) config_type documentation }
      name => COMPACT_STRING
      value => COMPACT_NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
      synonyms => { name value source }
        name => COMPACT_STRING
        value => COMPACT_NULLABLE_STRING
        source => INT8
      config_type => INT8
      documentation => COMPACT_NULLABLE_STRING
*/
export const DESCRIBE_CONFIGS_V4 = createApi({
    ...DESCRIBE_CONFIGS_V3,
    apiVersion: 4,
    fallback: DESCRIBE_CONFIGS_V3,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.resources, (encoder, resource) =>
                encoder
                    .writeInt8(resource.resourceType)
                    .writeCompactString(resource.resourceName)
                    .writeCompactArray(resource.configurationKeys, (encoder, configurationKey) =>
                        encoder.writeCompactString(configurationKey),
                    )
                    .writeTagBuffer(),
            )
            .writeBoolean(data.includeSynonyms)
            .writeBoolean(data.includeDocumentation ?? false)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readCompactArray((resultItem) => ({
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readCompactString(),
                resourceType: resultItem.readInt8(),
                resourceName: resultItem.readCompactString()!,
                configs: resultItem.readCompactArray((config) => ({
                    name: config.readCompactString()!,
                    value: config.readCompactString(),
                    readOnly: config.readBoolean(),
                    configSource: config.readInt8(),
                    isSensitive: config.readBoolean(),
                    synonyms: config.readCompactArray((synonym) => ({
                        name: synonym.readCompactString()!,
                        value: synonym.readCompactString(),
                        source: synonym.readInt8(),
                        tags: synonym.readTagBuffer(),
                    })),
                    configType: config.readInt8(),
                    documentation: config.readCompactString(),
                    tags: config.readTagBuffer(),
                })),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
