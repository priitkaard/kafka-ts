import { createApi } from '../../utils/api';
import { JOIN_GROUP_V2 } from './v2';

/*
JoinGroup Request (Version: 3) => { group_id session_timeout_ms rebalance_timeout_ms member_id protocol_type [protocols] }
  group_id => STRING
  session_timeout_ms => INT32
  rebalance_timeout_ms => INT32
  member_id => STRING
  protocol_type => STRING
  protocols => { name metadata }
    name => STRING
    metadata => BYTES

JoinGroup Response (Version: 3) => { throttle_time_ms error_code generation_id protocol_name leader member_id [members] }
  throttle_time_ms => INT32
  error_code => INT16
  generation_id => INT32
  protocol_name => STRING
  leader => STRING
  member_id => STRING
  members => { member_id metadata }
    member_id => STRING
    metadata => BYTES
*/
export const JOIN_GROUP_V3 = createApi({ ...JOIN_GROUP_V2, apiVersion: 3, fallback: JOIN_GROUP_V2 });
