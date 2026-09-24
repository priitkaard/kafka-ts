import { createApi } from '../../utils/api';
import { INIT_PRODUCER_ID_V0 } from './v0';

/*
InitProducerId Request (Version: 1) => { transactional_id transaction_timeout_ms }
  transactional_id => NULLABLE_STRING
  transaction_timeout_ms => INT32

InitProducerId Response (Version: 1) => { throttle_time_ms error_code producer_id producer_epoch }
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
export const INIT_PRODUCER_ID_V1 = createApi({ ...INIT_PRODUCER_ID_V0, apiVersion: 1, fallback: INIT_PRODUCER_ID_V0 });
