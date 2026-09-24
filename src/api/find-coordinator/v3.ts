import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { FIND_COORDINATOR_V2 } from './v2';

/*
FindCoordinator Request (Version: 3) => { key key_type }
  key => COMPACT_STRING
  key_type => INT8

FindCoordinator Response (Version: 3) => { throttle_time_ms error_code error_message node_id host port }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  node_id => INT32
  host => COMPACT_STRING
  port => INT32
*/
export const FIND_COORDINATOR_V3 = createApi({
    ...FIND_COORDINATOR_V2,
    apiVersion: 3,
    fallback: FIND_COORDINATOR_V2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) => encoder.writeCompactString(data.keys[0]).writeInt8(data.keyType).writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            coordinators: [
                {
                    key: '',
                    errorCode: decoder.readInt16(),
                    errorMessage: decoder.readCompactString(),
                    nodeId: decoder.readInt32(),
                    host: decoder.readCompactString()!,
                    port: decoder.readInt32(),
                    tags: {},
                },
            ],
            tags: decoder.readTagBuffer(),
        }),
});
