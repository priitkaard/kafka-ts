import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LIST_GROUPS_V3 } from './v3';

/*
ListGroups Request (Version: 4) => { (states_filter) }
  states_filter => COMPACT_STRING

ListGroups Response (Version: 4) => { throttle_time_ms error_code (groups) }
  throttle_time_ms => INT32
  error_code => INT16
  groups => { group_id protocol_type group_state }
    group_id => COMPACT_STRING
    protocol_type => COMPACT_STRING
    group_state => COMPACT_STRING
*/
export const LIST_GROUPS_V4 = createApi({
    ...LIST_GROUPS_V3,
    apiVersion: 4,
    fallback: LIST_GROUPS_V3,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.statesFilter ?? [], (encoder, statesFilter) =>
                encoder.writeCompactString(statesFilter),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            groups: decoder.readCompactArray((group) => ({
                groupId: group.readCompactString()!,
                protocolType: group.readCompactString()!,
                groupState: group.readCompactString()!,
                groupType: '',
                tags: group.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
