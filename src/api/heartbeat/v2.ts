import { createApi } from '../../utils/api';
import { HEARTBEAT_V1 } from './v1';

/*
Heartbeat Request (Version: 2) => { group_id generation_id member_id }
  group_id => STRING
  generation_id => INT32
  member_id => STRING

Heartbeat Response (Version: 2) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const HEARTBEAT_V2 = createApi({ ...HEARTBEAT_V1, apiVersion: 2, fallback: HEARTBEAT_V1 });
