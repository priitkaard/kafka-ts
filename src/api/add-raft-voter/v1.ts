import { createApi } from '../../utils/api';
import { ADD_RAFT_VOTER_V0 } from './v0';

/*
AddRaftVoter Request (Version: 1) => { cluster_id timeout_ms voter_id voter_directory_id (listeners) ack_when_committed }
  cluster_id => COMPACT_NULLABLE_STRING
  timeout_ms => INT32
  voter_id => INT32
  voter_directory_id => UUID
  listeners => { name host port }
    name => COMPACT_STRING
    host => COMPACT_STRING
    port => UINT16
  ack_when_committed => BOOLEAN

AddRaftVoter Response (Version: 1) => { throttle_time_ms error_code error_message }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
*/
export const ADD_RAFT_VOTER_V1 = createApi({
    ...ADD_RAFT_VOTER_V0,
    apiVersion: 1,
    fallback: ADD_RAFT_VOTER_V0,
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
            .writeBoolean(data.ackWhenCommitted ?? true)
            .writeTagBuffer(),
});
