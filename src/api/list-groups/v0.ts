import { createApi } from '../../utils/api';
import { ListGroupsRequest, ListGroupsResponse, throwIfError } from './common';

/*
ListGroups Request (Version: 0) => { }

ListGroups Response (Version: 0) => { error_code [groups] }
  error_code => INT16
  groups => { group_id protocol_type }
    group_id => STRING
    protocol_type => STRING
*/
export const LIST_GROUPS_V0 = createApi<ListGroupsRequest, ListGroupsResponse>({
    apiKey: 16,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder) => encoder,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
            errorCode: decoder.readInt16(),
            groups: decoder.readArray((group) => ({
                groupId: group.readString()!,
                protocolType: group.readString()!,
                groupState: '',
                groupType: '',
                tags: {},
            })),
            tags: {},
        }),
});
