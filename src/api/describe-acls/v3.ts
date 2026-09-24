import { createApi } from '../../utils/api';
import { DESCRIBE_ACLS_V2 } from './v2';

/*
DescribeAcls Request (Version: 3) => { resource_type_filter resource_name_filter pattern_type_filter principal_filter host_filter operation permission_type }
  resource_type_filter => INT8
  resource_name_filter => COMPACT_NULLABLE_STRING
  pattern_type_filter => INT8
  principal_filter => COMPACT_NULLABLE_STRING
  host_filter => COMPACT_NULLABLE_STRING
  operation => INT8
  permission_type => INT8

DescribeAcls Response (Version: 3) => { throttle_time_ms error_code error_message (resources) }
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
export const DESCRIBE_ACLS_V3 = createApi({ ...DESCRIBE_ACLS_V2, apiVersion: 3, fallback: DESCRIBE_ACLS_V2 });
