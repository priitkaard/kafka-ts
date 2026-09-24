import { createApi } from '../../utils/api';
import { INIT_PRODUCER_ID_V3 } from './v3';

/*
InitProducerId Request (Version: 4) => { transactional_id transaction_timeout_ms producer_id producer_epoch }
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16

InitProducerId Response (Version: 4) => { throttle_time_ms error_code producer_id producer_epoch }
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
export const INIT_PRODUCER_ID_V4 = createApi({ ...INIT_PRODUCER_ID_V3, apiVersion: 4, fallback: INIT_PRODUCER_ID_V3 });
