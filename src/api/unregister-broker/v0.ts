import { createApi } from '../../utils/api';
import { throwIfError, UnregisterBrokerRequest, UnregisterBrokerResponse } from './common';

/*
UnregisterBroker Request (Version: 0) => { broker_id }
  broker_id => INT32

UnregisterBroker Response (Version: 0) => { throttle_time_ms error_code error_message }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
*/
export const UNREGISTER_BROKER_V0 = createApi<UnregisterBrokerRequest, UnregisterBrokerResponse>({
    apiKey: 64,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) => encoder.writeInt32(data.brokerId).writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            tags: decoder.readTagBuffer(),
        }),
});
