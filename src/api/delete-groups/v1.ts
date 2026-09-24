import { createApi } from '../../utils/api';
import { DELETE_GROUPS_V0 } from './v0';

/*
DeleteGroups Request (Version: 1) => { [groups_names] }
  groups_names => STRING

DeleteGroups Response (Version: 1) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { group_id error_code }
    group_id => STRING
    error_code => INT16
*/
export const DELETE_GROUPS_V1 = createApi({ ...DELETE_GROUPS_V0, apiVersion: 1, fallback: DELETE_GROUPS_V0 });
