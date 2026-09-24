import { createApi } from '../../utils/api';
import { DescribeAclsRequest, DescribeAclsResponse, throwIfError } from './common';

/*
DescribeAcls Request (Version: 1) => { resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type }
  resource_type_filter => INT8
  resource_name_filter => NULLABLE_STRING
  pattern_type_filter => INT8
  principal_filter => NULLABLE_STRING
  host_filter => NULLABLE_STRING
  operation => INT8
  permission_type => INT8

DescribeAcls Response (Version: 1) => { throttle_time_ms error_code error_message [resources] }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  resources => { resource_type resource_name pattern_type [acls] }
    resource_type => INT8
    resource_name => STRING
    pattern_type => INT8
    acls => { principal host operation permission_type }
      principal => STRING
      host => STRING
      operation => INT8
      permission_type => INT8
*/
export const DESCRIBE_ACLS_V1 = createApi<DescribeAclsRequest, DescribeAclsResponse>({
    apiKey: 29,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeInt8(data.resourceTypeFilter)
            .writeString(data.resourceNameFilter)
            .writeInt8(data.patternTypeFilter)
            .writeString(data.principalFilter)
            .writeString(data.hostFilter)
            .writeInt8(data.operation)
            .writeInt8(data.permissionType),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readString(),
            resources: decoder.readArray((resource) => ({
                resourceType: resource.readInt8(),
                resourceName: resource.readString()!,
                patternType: resource.readInt8(),
                acls: resource.readArray((acl) => ({
                    principal: acl.readString()!,
                    host: acl.readString()!,
                    operation: acl.readInt8(),
                    permissionType: acl.readInt8(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
