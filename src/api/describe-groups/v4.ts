import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_GROUPS_V3 } from './v3';

/*
DescribeGroups Request (Version: 4) => { [groups] include_authorized_operations }
  groups => STRING
  include_authorized_operations => BOOLEAN

DescribeGroups Response (Version: 4) => { throttle_time_ms [groups] }
  throttle_time_ms => INT32
  groups => { error_code group_id group_state protocol_type protocol_data [members] authorized_operations }
    error_code => INT16
    group_id => STRING
    group_state => STRING
    protocol_type => STRING
    protocol_data => STRING
    members => { member_id group_instance_id client_id client_host member_metadata member_assignment }
      member_id => STRING
      group_instance_id => NULLABLE_STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES
    authorized_operations => INT32
*/
export const DESCRIBE_GROUPS_V4 = createApi({
    ...DESCRIBE_GROUPS_V3,
    apiVersion: 4,
    fallback: DESCRIBE_GROUPS_V3,
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
                    groupInstanceId: member.readString(),
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
