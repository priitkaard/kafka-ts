import { createApi } from '../../utils/api';
import { DELETE_ACLS_V2 } from './v2';

/*
DeleteAcls Request (Version: 3) => { (filters) }
  filters => { resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type }
    resource_type_filter => INT8
    resource_name_filter => COMPACT_NULLABLE_STRING
    pattern_type_filter => INT8
    principal_filter => COMPACT_NULLABLE_STRING
    host_filter => COMPACT_NULLABLE_STRING
    operation => INT8
    permission_type => INT8

DeleteAcls Response (Version: 3) => { throttle_time_ms (filter_results) }
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
export const DELETE_ACLS_V3 = createApi({ ...DELETE_ACLS_V2, apiVersion: 3, fallback: DELETE_ACLS_V2 });
