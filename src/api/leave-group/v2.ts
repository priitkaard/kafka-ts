import { createApi } from '../../utils/api';
import { LEAVE_GROUP_V1 } from './v1';

/*
LeaveGroup Request (Version: 2) => { group_id member_id }
  group_id => STRING
  member_id => STRING

LeaveGroup Response (Version: 2) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const LEAVE_GROUP_V2 = createApi({ ...LEAVE_GROUP_V1, apiVersion: 2, fallback: LEAVE_GROUP_V1 });
