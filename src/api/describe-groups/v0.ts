import { createApi } from '../../utils/api';
import { DescribeGroupsRequest, DescribeGroupsResponse, throwIfError } from './common';

/*
DescribeGroups Request (Version: 0) => { [groups] }
  groups => STRING

DescribeGroups Response (Version: 0) => { [groups] }
  groups => { error_code group_id group_state protocol_type protocol_data [members] }
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
*/
export const DESCRIBE_GROUPS_V0 = createApi<DescribeGroupsRequest, DescribeGroupsResponse>({
    apiKey: 15,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => encoder.writeArray(data.groups, (encoder, group) => encoder.writeString(group)),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
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
                authorizedOperations: -2147483648,
                tags: {},
            })),
            tags: {},
        }),
});
