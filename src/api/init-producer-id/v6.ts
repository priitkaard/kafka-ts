import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { INIT_PRODUCER_ID_V5 } from './v5';

/*
InitProducerId Request (Version: 6) => { transactional_id transaction_timeout_ms producer_id producer_epoch enable2_pc keep_prepared_txn }
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16
  enable2_pc => BOOLEAN
  keep_prepared_txn => BOOLEAN

InitProducerId Response (Version: 6) => { throttle_time_ms error_code producer_id producer_epoch ongoing_txn_producer_id ongoing_txn_producer_epoch }
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
  ongoing_txn_producer_id => INT64
  ongoing_txn_producer_epoch => INT16
*/
export const INIT_PRODUCER_ID_V6 = createApi({
    ...INIT_PRODUCER_ID_V5,
    apiVersion: 6,
    fallback: INIT_PRODUCER_ID_V5,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.transactionalId)
            .writeInt32(data.transactionTimeoutMs)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeBoolean(data.enable2Pc ?? false)
            .writeBoolean(data.keepPreparedTxn ?? false)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            producerId: decoder.readInt64(),
            producerEpoch: decoder.readInt16(),
            ongoingTxnProducerId: decoder.readInt64(),
            ongoingTxnProducerEpoch: decoder.readInt16(),
            tags: decoder.readTagBuffer(),
        }),
});
