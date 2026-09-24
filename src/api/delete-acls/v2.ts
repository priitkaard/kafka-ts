import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DELETE_ACLS_V1 } from './v1';

/*
DeleteAcls Request (Version: 2) => { (filters) }
  filters => { resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type }
    resource_type_filter => INT8
    resource_name_filter => COMPACT_NULLABLE_STRING
    pattern_type_filter => INT8
    principal_filter => COMPACT_NULLABLE_STRING
    host_filter => COMPACT_NULLABLE_STRING
    operation => INT8
    permission_type => INT8

DeleteAcls Response (Version: 2) => { throttle_time_ms (filter_results) }
  throttle_time_ms => INT32
  filter_results => { error_code error_message (matching_acls) }
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    matching_acls => { error_code error_message resource_type resource_name pattern_type principal host operation permission_type }
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      resource_type => INT8
      resource_name => COMPACT_STRING
      pattern_type => INT8
      principal => COMPACT_STRING
      host => COMPACT_STRING
      operation => INT8
      permission_type => INT8
*/
export const DELETE_ACLS_V2 = createApi({
    ...DELETE_ACLS_V1,
    apiVersion: 2,
    fallback: DELETE_ACLS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.filters, (encoder, filter) =>
                encoder
                    .writeInt8(filter.resourceTypeFilter)
                    .writeCompactString(filter.resourceNameFilter)
                    .writeInt8(filter.patternTypeFilter)
                    .writeCompactString(filter.principalFilter)
                    .writeCompactString(filter.hostFilter)
                    .writeInt8(filter.operation)
                    .writeInt8(filter.permissionType)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            filterResults: decoder.readCompactArray((filterResult) => ({
                errorCode: filterResult.readInt16(),
                errorMessage: filterResult.readCompactString(),
                matchingAcls: filterResult.readCompactArray((matchingAcl) => ({
                    errorCode: matchingAcl.readInt16(),
                    errorMessage: matchingAcl.readCompactString(),
                    resourceType: matchingAcl.readInt8(),
                    resourceName: matchingAcl.readCompactString()!,
                    patternType: matchingAcl.readInt8(),
                    principal: matchingAcl.readCompactString()!,
                    host: matchingAcl.readCompactString()!,
                    operation: matchingAcl.readInt8(),
                    permissionType: matchingAcl.readInt8(),
                    tags: matchingAcl.readTagBuffer(),
                })),
                tags: filterResult.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
