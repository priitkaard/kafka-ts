import { createApi } from '../../utils/api';
import { AddRaftVoterRequest, AddRaftVoterResponse, throwIfError } from './common';

/*
AddRaftVoter Request (Version: 0) => { cluster_id timeout_ms voter_id voter_directory_id (listeners) }
  cluster_id => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  voter_id => INT32
  voter_directory_id => UUID
  listeners => { name host port }
    name => COMPACT_STRING
    host => COMPACT_STRING
    port => UINT16

AddRaftVoter Response (Version: 0) => { throttle_time_ms error_code error_message }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
*/
export const ADD_RAFT_VOTER_V0 = createApi<AddRaftVoterRequest, AddRaftVoterResponse>({
    apiKey: 80,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.clusterId)
            .writeInt32(data.timeoutMs)
            .writeInt32(data.voterId)
            .writeUUID(data.voterDirectoryId)
            .writeCompactArray(data.listeners, (encoder, listener) =>
                encoder
                    .writeCompactString(listener.name)
                    .writeCompactString(listener.host)
                    .writeUInt16(listener.port)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            tags: decoder.readTagBuffer(),
        }),
});
