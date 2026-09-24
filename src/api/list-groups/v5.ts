import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LIST_GROUPS_V4 } from './v4';

/*
ListGroups Request (Version: 5) => { (states_filter) (types_filter) }
  states_filter => COMPACT_STRING
  types_filter => COMPACT_STRING

ListGroups Response (Version: 5) => { throttle_time_ms error_code (groups) }
  throttle_time_ms => INT32
  error_code => INT16
  groups => { group_id protocol_type group_state group_type }
    group_id => COMPACT_STRING
    protocol_type => COMPACT_STRING
    group_state => COMPACT_STRING
    group_type => COMPACT_STRING
*/
export const LIST_GROUPS_V5 = createApi({
    ...LIST_GROUPS_V4,
    apiVersion: 5,
    fallback: LIST_GROUPS_V4,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.statesFilter ?? [], (encoder, statesFilter) =>
                encoder.writeCompactString(statesFilter),
            )
            .writeCompactArray(data.typesFilter ?? [], (encoder, typesFilter) =>
                encoder.writeCompactString(typesFilter),
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
                groupType: group.readCompactString()!,
                tags: group.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
