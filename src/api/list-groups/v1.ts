import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LIST_GROUPS_V0 } from './v0';

/*
ListGroups Request (Version: 1) => { }

ListGroups Response (Version: 1) => { throttle_time_ms error_code [groups] }
  throttle_time_ms => INT32
  error_code => INT16
  groups => { group_id protocol_type }
    group_id => STRING
    protocol_type => STRING
*/
export const LIST_GROUPS_V1 = createApi({
    ...LIST_GROUPS_V0,
    apiVersion: 1,
    fallback: LIST_GROUPS_V0,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
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
