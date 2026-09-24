import { createApi } from '../../utils/api';
import { RemoveRaftVoterRequest, RemoveRaftVoterResponse, throwIfError } from './common';

/*
RemoveRaftVoter Request (Version: 0) => { cluster_id voter_id voter_directory_id }
  cluster_id => COMPACT_NULLABLE_STRING
  voter_id => INT32
  voter_directory_id => UUID

RemoveRaftVoter Response (Version: 0) => { throttle_time_ms error_code error_message }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
*/
export const REMOVE_RAFT_VOTER_V0 = createApi<RemoveRaftVoterRequest, RemoveRaftVoterResponse>({
    apiKey: 81,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.clusterId)
            .writeInt32(data.voterId)
            .writeUUID(data.voterDirectoryId)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            tags: decoder.readTagBuffer(),
        }),
});
