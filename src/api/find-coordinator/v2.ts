import { createApi } from '../../utils/api';
import { FIND_COORDINATOR_V1 } from './v1';

/*
FindCoordinator Request (Version: 2) => { key key_type }
  key => STRING
  key_type => INT8

FindCoordinator Response (Version: 2) => { throttle_time_ms error_code error_message node_id host port }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  node_id => INT32
  host => STRING
  port => INT32
*/
export const FIND_COORDINATOR_V2 = createApi({ ...FIND_COORDINATOR_V1, apiVersion: 2, fallback: FIND_COORDINATOR_V1 });
