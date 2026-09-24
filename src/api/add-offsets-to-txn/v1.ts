import { createApi } from '../../utils/api';
import { ADD_OFFSETS_TO_TXN_V0 } from './v0';

/*
AddOffsetsToTxn Request (Version: 1) => { transactional_id producer_id producer_epoch group_id }
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => STRING

AddOffsetsToTxn Response (Version: 1) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const ADD_OFFSETS_TO_TXN_V1 = createApi({
    ...ADD_OFFSETS_TO_TXN_V0,
    apiVersion: 1,
    fallback: ADD_OFFSETS_TO_TXN_V0,
});
