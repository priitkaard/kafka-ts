import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const INIT_PRODUCER_ID = createApi({
    apiKey: 22,
    apiVersion: 4,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (
        encoder,
        body: {
            transactionalId: string | null;
            transactionTimeoutMs: number;
            producerId: bigint;
            producerEpoch: number;
        },
    ) =>
        encoder
            .writeCompactString(body.transactionalId)
            .writeInt32(body.transactionTimeoutMs)
            .writeInt64(body.producerId)
            .writeInt16(body.producerEpoch)
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
