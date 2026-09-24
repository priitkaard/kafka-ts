import { createApi } from '../../utils/api';
import { AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, throwIfError } from './common';

/*
AddOffsetsToTxn Request (Version: 0) => { transactional_id producer_id producer_epoch group_id }
  transactional_id => STRING
  producer_id => INT64
  producer_epoch => INT16
  group_id => STRING

AddOffsetsToTxn Response (Version: 0) => { throttle_time_ms error_code }
  throttle_time_ms => INT32
  error_code => INT16
*/
export const ADD_OFFSETS_TO_TXN_V0 = createApi<AddOffsetsToTxnRequest, AddOffsetsToTxnResponse>({
    apiKey: 25,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.transactionalId)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeString(data.groupId),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            tags: {},
        }),
});
