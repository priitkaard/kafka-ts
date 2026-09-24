import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { FIND_COORDINATOR_V0 } from './v0';

/*
FindCoordinator Request (Version: 1) => { key key_type }
  key => STRING
  key_type => INT8

FindCoordinator Response (Version: 1) => { throttle_time_ms error_code error_message node_id host port }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  node_id => INT32
  host => STRING
  port => INT32
*/
export const FIND_COORDINATOR_V1 = createApi({
    ...FIND_COORDINATOR_V0,
    apiVersion: 1,
    fallback: FIND_COORDINATOR_V0,
    request: (encoder, data) => encoder.writeString(data.keys[0]).writeInt8(data.keyType),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            coordinators: [
                {
                    key: '',
                    errorCode: decoder.readInt16(),
                    errorMessage: decoder.readString(),
                    nodeId: decoder.readInt32(),
                    host: decoder.readString()!,
                    port: decoder.readInt32(),
                    tags: {},
                },
            ],
            tags: {},
        }),
});
