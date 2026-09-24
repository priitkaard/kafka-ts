import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_GROUPS_V4 } from './v4';

/*
DescribeGroups Request (Version: 5) => { (groups) include_authorized_operations }
  groups => COMPACT_STRING
  include_authorized_operations => BOOLEAN

DescribeGroups Response (Version: 5) => { throttle_time_ms (groups) }
  throttle_time_ms => INT32
  groups => { error_code group_id group_state protocol_type protocol_data (members) authorized_operations }
    error_code => INT16
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    protocol_type => COMPACT_STRING
    protocol_data => COMPACT_STRING
    members => { member_id group_instance_id client_id client_host member_metadata member_assignment }
      member_id => COMPACT_STRING
      group_instance_id => COMPACT_NULLABLE_STRING
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      member_metadata => COMPACT_BYTES
      member_assignment => COMPACT_BYTES
    authorized_operations => INT32
*/
export const DESCRIBE_GROUPS_V5 = createApi({
    ...DESCRIBE_GROUPS_V4,
    apiVersion: 5,
    fallback: DESCRIBE_GROUPS_V4,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.groups, (encoder, group) => encoder.writeCompactString(group))
            .writeBoolean(data.includeAuthorizedOperations ?? false)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            groups: decoder.readCompactArray((group) => ({
                errorCode: group.readInt16(),
                errorMessage: null,
                groupId: group.readCompactString()!,
                groupState: group.readCompactString()!,
                protocolType: group.readCompactString()!,
                protocolData: group.readCompactString()!,
                members: group.readCompactArray((member) => ({
                    memberId: member.readCompactString()!,
                    groupInstanceId: member.readCompactString(),
                    clientId: member.readCompactString()!,
                    clientHost: member.readCompactString()!,
                    memberMetadata: member.readCompactBytes()!,
                    memberAssignment: member.readCompactBytes()!,
                    tags: member.readTagBuffer(),
                })),
                authorizedOperations: group.readInt32(),
                tags: group.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
