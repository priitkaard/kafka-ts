import { createApi } from '../../utils/api';
import { LIST_GROUPS_V1 } from './v1';

/*
ListGroups Request (Version: 2) => { }

ListGroups Response (Version: 2) => { throttle_time_ms error_code [groups] }
  throttle_time_ms => INT32
  error_code => INT16
  groups => { group_id protocol_type }
    group_id => STRING
    protocol_type => STRING
*/
export const LIST_GROUPS_V2 = createApi({ ...LIST_GROUPS_V1, apiVersion: 2, fallback: LIST_GROUPS_V1 });
