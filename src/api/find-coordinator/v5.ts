import { createApi } from '../../utils/api';
import { FIND_COORDINATOR_V4 } from './v4';

/*
FindCoordinator Request (Version: 5) => { key_type (coordinator_keys) }
  key_type => INT8
  coordinator_keys => COMPACT_STRING

FindCoordinator Response (Version: 5) => { throttle_time_ms (coordinators) }
  throttle_time_ms => INT32
  coordinators => { key node_id host port error_code error_message }
    key => COMPACT_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const FIND_COORDINATOR_V5 = createApi({ ...FIND_COORDINATOR_V4, apiVersion: 5, fallback: FIND_COORDINATOR_V4 });
