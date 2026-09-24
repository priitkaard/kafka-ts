import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_GROUPS_V2 } from './v2';

/*
DescribeGroups Request (Version: 3) => { [groups] include_authorized_operations }
  groups => STRING
  include_authorized_operations => BOOLEAN

DescribeGroups Response (Version: 3) => { throttle_time_ms [groups] }
  throttle_time_ms => INT32
  groups => { error_code group_id group_state protocol_type protocol_data [members] authorized_operations }
    error_code => INT16
    group_id => STRING
    group_state => STRING
    protocol_type => STRING
    protocol_data => STRING
    members => { member_id client_id client_host member_metadata member_assignment }
      member_id => STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES
    authorized_operations => INT32
*/
export const DESCRIBE_GROUPS_V3 = createApi({
    ...DESCRIBE_GROUPS_V2,
    apiVersion: 3,
    fallback: DESCRIBE_GROUPS_V2,
    request: (encoder, data) =>
        encoder
            .writeArray(data.groups, (encoder, group) => encoder.writeString(group))
            .writeBoolean(data.includeAuthorizedOperations ?? false),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            groups: decoder.readArray((group) => ({
                errorCode: group.readInt16(),
                errorMessage: null,
                groupId: group.readString()!,
                groupState: group.readString()!,
                protocolType: group.readString()!,
                protocolData: group.readString()!,
                members: group.readArray((member) => ({
                    memberId: member.readString()!,
                    groupInstanceId: null,
                    clientId: member.readString()!,
                    clientHost: member.readString()!,
                    memberMetadata: member.readBytes()!,
                    memberAssignment: member.readBytes()!,
                    tags: {},
                })),
                authorizedOperations: group.readInt32(),
                tags: {},
            })),
            tags: {},
        }),
});
