import { createApi } from '../../utils/api';
import { END_TXN_V3 } from './v3';

/*
EndTxn Request (Version: 4) => { transactional_id producer_id producer_epoch committed }
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN

EndTxn Response (Version: 4) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const END_TXN_V4 = createApi({ ...END_TXN_V3, apiVersion: 4, fallback: END_TXN_V3 });
