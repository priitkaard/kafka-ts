import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LIST_GROUPS_V2 } from './v2';

/*
ListGroups Request (Version: 3) => { }

ListGroups Response (Version: 3) => { throttle_time_ms error_code (groups) }
  throttle_time_ms => INT32
  error_code => INT16
  groups => { group_id protocol_type }
    group_id => COMPACT_STRING
    protocol_type => COMPACT_STRING
*/
export const LIST_GROUPS_V3 = createApi({
    ...LIST_GROUPS_V2,
    apiVersion: 3,
    fallback: LIST_GROUPS_V2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder) => encoder.writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            groups: decoder.readCompactArray((group) => ({
                groupId: group.readCompactString()!,
                protocolType: group.readCompactString()!,
                groupState: '',
                groupType: '',
                tags: group.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
