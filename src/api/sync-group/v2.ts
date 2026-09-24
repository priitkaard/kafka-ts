import { createApi } from '../../utils/api';
import { SYNC_GROUP_V1 } from './v1';

/*
SyncGroup Request (Version: 2) => { group_id generation_id member_id [assignments] }
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  assignments => { member_id assignment }
    member_id => STRING
    assignment => BYTES

SyncGroup Response (Version: 2) => { throttle_time_ms error_code assignment }
  throttle_time_ms => INT32
  error_code => INT16
  assignment => BYTES
*/
export const SYNC_GROUP_V2 = createApi({ ...SYNC_GROUP_V1, apiVersion: 2, fallback: SYNC_GROUP_V1 });
