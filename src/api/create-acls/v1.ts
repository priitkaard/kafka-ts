import { createApi } from '../../utils/api';
import { CreateAclsRequest, CreateAclsResponse, throwIfError } from './common';

/*
CreateAcls Request (Version: 1) => { [creations] }
  creations => { resource_type resource_name resource_pattern_type principal host operation permission_type }
    resource_type => INT8
    resource_name => STRING
    resource_pattern_type => INT8
    principal => STRING
    host => STRING
    operation => INT8
    permission_type => INT8

CreateAcls Response (Version: 1) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { error_code error_message }
    error_code => INT16
    error_message => NULLABLE_STRING
*/
export const CREATE_ACLS_V1 = createApi<CreateAclsRequest, CreateAclsResponse>({
    apiKey: 30,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.creations, (encoder, creation) =>
            encoder
                .writeInt8(creation.resourceType)
                .writeString(creation.resourceName)
                .writeInt8(creation.resourcePatternType)
                .writeString(creation.principal)
                .writeString(creation.host)
                .writeInt8(creation.operation)
                .writeInt8(creation.permissionType),
        ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readArray((resultItem) => ({
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readString(),
                tags: {},
            })),
            tags: {},
        }),
});
