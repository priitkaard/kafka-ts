import { createApi } from '../../utils/api';
import { END_TXN_V0 } from './v0';

/*
EndTxn Request (Version: 1) => { transactional_id producer_id producer_epoch committed }
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  committed => BOOLEAN

EndTxn Response (Version: 1) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const END_TXN_V1 = createApi({ ...END_TXN_V0, apiVersion: 1, fallback: END_TXN_V0 });
