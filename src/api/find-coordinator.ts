import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const KEY_TYPE = {
    GROUP: 0,
    TRANSACTION: 1,
};

type FindCoordinatorRequest = {
    keyType: number;
    keys: string[];
};

type FindCoordinatorResponse = {
    throttleTimeMs: number;
    coordinators: {
        key: string;
        nodeId: number;
        host: string;
        port: number;
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

/*
FindCoordinator Request (Version: 1) => key key_type 
  key => STRING
  key_type => INT8

FindCoordinator Response (Version: 1) => throttle_time_ms error_code error_message node_id host port 
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  node_id => INT32
  host => STRING
  port => INT32
*/
const FIND_COORDINATOR_V1 = createApi<FindCoordinatorRequest, FindCoordinatorResponse>({
    apiKey: 10,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeString(data.keys[0]).writeInt8(data.keyType),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readString(),
            coordinators: [
                {
                    key: '', // Key not present in v1 response
                    nodeId: decoder.readInt32(),
                    host: decoder.readString()!,
                    port: decoder.readInt32(),
                    errorCode: 0,
                    errorMessage: null,
                    tags: {},
                },
            ],
            tags: {},
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
        return result;
    },
});

/*
FindCoordinator Request (Version: 4) => key_type [coordinator_keys] _tagged_fields 
  key_type => INT8
  coordinator_keys => COMPACT_STRING

FindCoordinator Response (Version: 4) => throttle_time_ms [coordinators] _tagged_fields 
  throttle_time_ms => INT32
  coordinators => key node_id host port error_code error_message _tagged_fields 
    key => COMPACT_STRING
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const FIND_COORDINATOR = createApi<FindCoordinatorRequest, FindCoordinatorResponse>({
    apiKey: 10,
    apiVersion: 4,
    fallback: FIND_COORDINATOR_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt8(data.keyType)
            .writeCompactArray(data.keys, (encoder, key) => encoder.writeCompactString(key))
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            coordinators: decoder.readCompactArray((decoder) => ({
                key: decoder.readCompactString()!,
                nodeId: decoder.readInt32(),
                host: decoder.readCompactString()!,
                port: decoder.readInt32(),
                errorCode: decoder.readInt16(),
                errorMessage: decoder.readCompactString(),
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        result.coordinators.forEach((coordinator) => {
            if (coordinator.errorCode)
                throw new KafkaTSApiError(coordinator.errorCode, coordinator.errorMessage, result);
        });
        return result;
    },
});
