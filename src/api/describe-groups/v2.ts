import { createApi } from '../../utils/api';
import { DESCRIBE_GROUPS_V1 } from './v1';

/*
DescribeGroups Request (Version: 2) => { [groups] }
  groups => STRING

DescribeGroups Response (Version: 2) => { throttle_time_ms [groups] }
  throttle_time_ms => INT32
  groups => { error_code group_id group_state protocol_type protocol_data [members] }
    error_code => INT16
    group_id => STRING
    group_state => STRING
    protocol_type => STRING
    protocol_data => STRING
    members => { member_id client_id client_host member_metadata member_assignment }
      member_id => STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES
*/
export const DESCRIBE_GROUPS_V2 = createApi({ ...DESCRIBE_GROUPS_V1, apiVersion: 2, fallback: DESCRIBE_GROUPS_V1 });
