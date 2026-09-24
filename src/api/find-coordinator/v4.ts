import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { FIND_COORDINATOR_V3 } from './v3';

/*
FindCoordinator Request (Version: 4) => { key_type (coordinator_keys) }
  key_type => INT8
  coordinator_keys => COMPACT_STRING

FindCoordinator Response (Version: 4) => { throttle_time_ms (coordinators) }
  throttle_time_ms => INT32
  coordinators => { key node_id host port error_code error_message }
    key => COMPACT_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const FIND_COORDINATOR_V4 = createApi({
    ...FIND_COORDINATOR_V3,
    apiVersion: 4,
    fallback: FIND_COORDINATOR_V3,
    request: (encoder, data) =>
        encoder
            .writeInt8(data.keyType)
            .writeCompactArray(data.keys, (encoder, key) => encoder.writeCompactString(key))
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            coordinators: decoder.readCompactArray((coordinator) => ({
                key: coordinator.readCompactString()!,
                nodeId: coordinator.readInt32(),
                host: coordinator.readCompactString()!,
                port: coordinator.readInt32(),
                errorCode: coordinator.readInt16(),
                errorMessage: coordinator.readCompactString(),
                tags: coordinator.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
