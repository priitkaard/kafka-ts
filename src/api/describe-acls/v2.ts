import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_ACLS_V1 } from './v1';

/*
DescribeAcls Request (Version: 2) => { resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type }
  resource_type_filter => INT8
  resource_name_filter => COMPACT_NULLABLE_STRING
  pattern_type_filter => INT8
  principal_filter => COMPACT_NULLABLE_STRING
  host_filter => COMPACT_NULLABLE_STRING
  operation => INT8
  permission_type => INT8

DescribeAcls Response (Version: 2) => { throttle_time_ms error_code error_message (resources) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  resources => { resource_type resource_name pattern_type (acls) }
    resource_type => INT8
    resource_name => COMPACT_STRING
    pattern_type => INT8
    acls => { principal host operation permission_type }
      principal => COMPACT_STRING
      host => COMPACT_STRING
      operation => INT8
      permission_type => INT8
*/
export const DESCRIBE_ACLS_V2 = createApi({
    ...DESCRIBE_ACLS_V1,
    apiVersion: 2,
    fallback: DESCRIBE_ACLS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt8(data.resourceTypeFilter)
            .writeCompactString(data.resourceNameFilter)
            .writeInt8(data.patternTypeFilter)
            .writeCompactString(data.principalFilter)
            .writeCompactString(data.hostFilter)
            .writeInt8(data.operation)
            .writeInt8(data.permissionType)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            resources: decoder.readCompactArray((resource) => ({
                resourceType: resource.readInt8(),
                resourceName: resource.readCompactString()!,
                patternType: resource.readInt8(),
                acls: resource.readCompactArray((acl) => ({
                    principal: acl.readCompactString()!,
                    host: acl.readCompactString()!,
                    operation: acl.readInt8(),
                    permissionType: acl.readInt8(),
                    tags: acl.readTagBuffer(),
                })),
                tags: resource.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
