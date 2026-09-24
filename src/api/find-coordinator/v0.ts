import { createApi } from '../../utils/api';
import { FindCoordinatorRequest, FindCoordinatorResponse, throwIfError } from './common';

/*
FindCoordinator Request (Version: 0) => { key }
  key => STRING

FindCoordinator Response (Version: 0) => { error_code node_id host port }
  error_code => INT16
  node_id => INT32
  host => STRING
  port => INT32
*/
export const FIND_COORDINATOR_V0 = createApi<FindCoordinatorRequest, FindCoordinatorResponse>({
    apiKey: 10,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeString(data.keys[0]),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
            coordinators: [
                {
                    key: '',
                    errorCode: decoder.readInt16(),
                    errorMessage: null,
                    nodeId: decoder.readInt32(),
                    host: decoder.readString()!,
                    port: decoder.readInt32(),
                    tags: {},
                },
            ],
            tags: {},
        }),
});
