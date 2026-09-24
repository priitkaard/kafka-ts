import { createApi } from '../../utils/api';
import { INIT_PRODUCER_ID_V4 } from './v4';

/*
InitProducerId Request (Version: 5) => { transactional_id transaction_timeout_ms producer_id producer_epoch }
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16

InitProducerId Response (Version: 5) => { throttle_time_ms error_code producer_id producer_epoch }
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
export const INIT_PRODUCER_ID_V5 = createApi({ ...INIT_PRODUCER_ID_V4, apiVersion: 5, fallback: INIT_PRODUCER_ID_V4 });
