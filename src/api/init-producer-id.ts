import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type InitProducerIdRequest = {
    transactionalId: string | null;
    transactionTimeoutMs: number;
    producerId: bigint;
    producerEpoch: number;
};

type InitProducerIdResponse = {
    throttleTimeMs: number;
    errorCode: number;
    producerId: bigint;
    producerEpoch: number;
    tags: Record<number, Buffer>;
};

/*
InitProducerId Request (Version: 0) => transactional_id transaction_timeout_ms 
  transactional_id => NULLABLE_STRING
  transaction_timeout_ms => INT32

InitProducerId Response (Version: 0) => throttle_time_ms error_code producer_id producer_epoch 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
const INIT_PRODUCER_ID_V0 = createApi<InitProducerIdRequest, InitProducerIdResponse>({
    apiKey: 22,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeString(data.transactionalId).writeInt32(data.transactionTimeoutMs),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            producerId: decoder.readInt64(),
            producerEpoch: decoder.readInt16(),
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});

/*
InitProducerId Request (Version: 4) => transactional_id transaction_timeout_ms producer_id producer_epoch _tagged_fields 
  transactional_id => COMPACT_NULLABLE_STRING
  transaction_timeout_ms => INT32
  producer_id => INT64
  producer_epoch => INT16

InitProducerId Response (Version: 4) => throttle_time_ms error_code producer_id producer_epoch _tagged_fields 
  throttle_time_ms => INT32
  error_code => INT16
  producer_id => INT64
  producer_epoch => INT16
*/
export const INIT_PRODUCER_ID = createApi<InitProducerIdRequest, InitProducerIdResponse>({
    apiKey: 22,
    apiVersion: 4,
    fallback: INIT_PRODUCER_ID_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.transactionalId)
            .writeInt32(data.transactionTimeoutMs)
            .writeInt64(data.producerId)
            .writeInt16(data.producerEpoch)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            producerId: decoder.readInt64(),
            producerEpoch: decoder.readInt16(),
            tags: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
