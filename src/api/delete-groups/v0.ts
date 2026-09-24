import { createApi } from '../../utils/api';
import { DeleteGroupsRequest, DeleteGroupsResponse, throwIfError } from './common';

/*
DeleteGroups Request (Version: 0) => { [groups_names] }
  groups_names => STRING

DeleteGroups Response (Version: 0) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { group_id error_code }
    group_id => STRING
    error_code => INT16
*/
export const DELETE_GROUPS_V0 = createApi<DeleteGroupsRequest, DeleteGroupsResponse>({
    apiKey: 42,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.groupsNames, (encoder, groupsName) => encoder.writeString(groupsName)),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readArray((resultItem) => ({
                groupId: resultItem.readString()!,
                errorCode: resultItem.readInt16(),
                tags: {},
            })),
            tags: {},
        }),
});
