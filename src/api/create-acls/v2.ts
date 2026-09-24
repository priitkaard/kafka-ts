import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { CREATE_ACLS_V1 } from './v1';

/*
CreateAcls Request (Version: 2) => { (creations) }
  creations => { resource_type resource_name resource_pattern_type principal host operation permission_type }
    resource_type => INT8
    resource_name => COMPACT_STRING
    resource_pattern_type => INT8
    principal => COMPACT_STRING
    host => COMPACT_STRING
    operation => INT8
    permission_type => INT8

CreateAcls Response (Version: 2) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { error_code error_message }
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const CREATE_ACLS_V2 = createApi({
    ...CREATE_ACLS_V1,
    apiVersion: 2,
    fallback: CREATE_ACLS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.creations, (encoder, creation) =>
                encoder
                    .writeInt8(creation.resourceType)
                    .writeCompactString(creation.resourceName)
                    .writeInt8(creation.resourcePatternType)
                    .writeCompactString(creation.principal)
                    .writeCompactString(creation.host)
                    .writeInt8(creation.operation)
                    .writeInt8(creation.permissionType)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readCompactArray((resultItem) => ({
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readCompactString(),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
