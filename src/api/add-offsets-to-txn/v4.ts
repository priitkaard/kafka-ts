import { createApi } from '../../utils/api';
import { ADD_OFFSETS_TO_TXN_V3 } from './v3';

/*
AddOffsetsToTxn Request (Version: 4) => { transactional_id producer_id producer_epoch group_id }
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => COMPACT_STRING

AddOffsetsToTxn Response (Version: 4) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const ADD_OFFSETS_TO_TXN_V4 = createApi({
    ...ADD_OFFSETS_TO_TXN_V3,
    apiVersion: 4,
    fallback: ADD_OFFSETS_TO_TXN_V3,
});
