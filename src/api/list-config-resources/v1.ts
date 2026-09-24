import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LIST_CONFIG_RESOURCES_V0 } from './v0';

/*
ListConfigResources Request (Version: 1) => { (resource_types) }
  resource_types => INT8

ListConfigResources Response (Version: 1) => { throttle_time_ms error_code (config_resources) }
  throttle_time_ms => INT32
  error_code => INT16
  config_resources => { resource_name resource_type }
    resource_name => COMPACT_STRING
    resource_type => INT8
*/
export const LIST_CONFIG_RESOURCES_V1 = createApi({
    ...LIST_CONFIG_RESOURCES_V0,
    apiVersion: 1,
    fallback: LIST_CONFIG_RESOURCES_V0,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.resourceTypes ?? [], (encoder, resourceType) => encoder.writeInt8(resourceType))
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            configResources: decoder.readCompactArray((configResource) => ({
                resourceName: configResource.readCompactString()!,
                resourceType: configResource.readInt8(),
                tags: configResource.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
