import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DELETE_GROUPS_V1 } from './v1';

/*
DeleteGroups Request (Version: 2) => { (groups_names) }
  groups_names => COMPACT_STRING

DeleteGroups Response (Version: 2) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { group_id error_code }
    group_id => COMPACT_STRING
    error_code => INT16
*/
export const DELETE_GROUPS_V2 = createApi({
    ...DELETE_GROUPS_V1,
    apiVersion: 2,
    fallback: DELETE_GROUPS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.groupsNames, (encoder, groupsName) => encoder.writeCompactString(groupsName))
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readCompactArray((resultItem) => ({
                groupId: resultItem.readCompactString()!,
                errorCode: resultItem.readInt16(),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
