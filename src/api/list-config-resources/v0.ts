import { createApi } from '../../utils/api';
import { ListConfigResourcesRequest, ListConfigResourcesResponse, throwIfError } from './common';

/*
ListConfigResources Request (Version: 0) => { }

ListConfigResources Response (Version: 0) => { throttle_time_ms error_code (config_resources) }
  throttle_time_ms => INT32
  error_code => INT16
  config_resources => { resource_name }
    resource_name => COMPACT_STRING
*/
export const LIST_CONFIG_RESOURCES_V0 = createApi<ListConfigResourcesRequest, ListConfigResourcesResponse>({
    apiKey: 74,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder) => encoder.writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            configResources: decoder.readCompactArray((configResource) => ({
                resourceName: configResource.readCompactString()!,
                resourceType: 16,
                tags: configResource.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
