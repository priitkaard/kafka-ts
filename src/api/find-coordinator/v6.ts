import { createApi } from '../../utils/api';
import { FIND_COORDINATOR_V5 } from './v5';

/*
FindCoordinator Request (Version: 6) => { key_type (coordinator_keys) }
  key_type => INT8
  coordinator_keys => COMPACT_STRING

FindCoordinator Response (Version: 6) => { throttle_time_ms (coordinators) }
  throttle_time_ms => INT32
  coordinators => { key node_id host port error_code error_message }
    key => COMPACT_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const FIND_COORDINATOR_V6 = createApi({ ...FIND_COORDINATOR_V5, apiVersion: 6, fallback: FIND_COORDINATOR_V5 });
