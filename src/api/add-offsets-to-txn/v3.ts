import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { ADD_OFFSETS_TO_TXN_V2 } from './v2';

/*
AddOffsetsToTxn Request (Version: 3) => { transactional_id producer_id producer_epoch group_id }
  transactional_id => COMPACT_STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => COMPACT_STRING

AddOffsetsToTxn Response (Version: 3) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const ADD_OFFSETS_TO_TXN_V3 = createApi({
    ...ADD_OFFSETS_TO_TXN_V2,
    apiVersion: 3,
    fallback: ADD_OFFSETS_TO_TXN_V2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.transactionalId)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeCompactString(data.groupId)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            tags: decoder.readTagBuffer(),
        }),
});
