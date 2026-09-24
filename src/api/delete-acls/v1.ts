import { createApi } from '../../utils/api';
import { DeleteAclsRequest, DeleteAclsResponse, throwIfError } from './common';

/*
DeleteAcls Request (Version: 1) => { [filters] }
  filters => { resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type }
    resource_type_filter => INT8
    resource_name_filter => NULLABLE_STRING
    pattern_type_filter => INT8
    principal_filter => NULLABLE_STRING
    host_filter => NULLABLE_STRING
    operation => INT8
    permission_type => INT8

DeleteAcls Response (Version: 1) => { throttle_time_ms [filter_results] }
  throttle_time_ms => INT32
  filter_results => { error_code error_message [matching_acls] }
    error_code => INT16
    error_message => NULLABLE_STRING
    matching_acls => { error_code error_message resource_type resource_name pattern_type principal host operation permission_type }
      error_code => INT16
      error_message => NULLABLE_STRING
      resource_type => INT8
      resource_name => STRING
      pattern_type => INT8
      principal => STRING
      host => STRING
      operation => INT8
      permission_type => INT8
*/
export const DELETE_ACLS_V1 = createApi<DeleteAclsRequest, DeleteAclsResponse>({
    apiKey: 31,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.filters, (encoder, filter) =>
            encoder
                .writeInt8(filter.resourceTypeFilter)
                .writeString(filter.resourceNameFilter)
                .writeInt8(filter.patternTypeFilter)
                .writeString(filter.principalFilter)
                .writeString(filter.hostFilter)
                .writeInt8(filter.operation)
                .writeInt8(filter.permissionType),
        ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            filterResults: decoder.readArray((filterResult) => ({
                errorCode: filterResult.readInt16(),
                errorMessage: filterResult.readString(),
                matchingAcls: filterResult.readArray((matchingAcl) => ({
                    errorCode: matchingAcl.readInt16(),
                    errorMessage: matchingAcl.readString(),
                    resourceType: matchingAcl.readInt8(),
                    resourceName: matchingAcl.readString()!,
                    patternType: matchingAcl.readInt8(),
                    principal: matchingAcl.readString()!,
                    host: matchingAcl.readString()!,
                    operation: matchingAcl.readInt8(),
                    permissionType: matchingAcl.readInt8(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
