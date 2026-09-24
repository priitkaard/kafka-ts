import { createApi } from '../../utils/api';
import { decodeAssignment, encodeAssignment, SyncGroupRequest, SyncGroupResponse, throwIfError } from './common';

/*
SyncGroup Request (Version: 0) => { group_id generation_id member_id [assignments] }
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  assignments => { member_id assignment }
    member_id => STRING
    assignment => BYTES

SyncGroup Response (Version: 0) => { error_code assignment }
  error_code => INT16
  assignment => BYTES
*/
export const SYNC_GROUP_V0 = createApi<SyncGroupRequest, SyncGroupResponse>({
    apiKey: 14,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.generationId)
            .writeString(data.memberId)
            .writeArray(data.assignments, (encoder, assignment) =>
                encoder.writeString(assignment.memberId).writeBytes(encodeAssignment(assignment.assignment)),
            ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
            errorCode: decoder.readInt16(),
            protocolType: null,
            protocolName: null,
            assignment: decodeAssignment(decoder.readBytes()),
            tags: {},
        }),
});
