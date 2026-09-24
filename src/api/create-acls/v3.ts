import { createApi } from '../../utils/api';
import { CREATE_ACLS_V2 } from './v2';

/*
CreateAcls Request (Version: 3) => { (creations) }
  creations => { resource_type resource_name resource_pattern_type principal host operation permission_type }
    resource_type => INT8
    resource_name => COMPACT_STRING
    resource_pattern_type => INT8
    principal => COMPACT_STRING
    host => COMPACT_STRING
    operation => INT8
    permission_type => INT8

CreateAcls Response (Version: 3) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { error_code error_message }
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const CREATE_ACLS_V3 = createApi({ ...CREATE_ACLS_V2, apiVersion: 3, fallback: CREATE_ACLS_V2 });
