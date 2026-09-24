import { createApi } from '../../utils/api';
import { HeartbeatRequest, HeartbeatResponse, throwIfError } from './common';

/*
Heartbeat Request (Version: 0) => { group_id generation_id member_id }
  group_id => STRING
  generation_id => INT32
  member_id => STRING

Heartbeat Response (Version: 0) => { error_code }
  error_code => INT16
*/
export const HEARTBEAT_V0 = createApi<HeartbeatRequest, HeartbeatResponse>({
    apiKey: 12,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeString(data.groupId).writeInt32(data.generationId).writeString(data.memberId),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
            errorCode: decoder.readInt16(),
            tags: {},
        }),
});
