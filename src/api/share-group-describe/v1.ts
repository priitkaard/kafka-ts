import { createApi } from '../../utils/api';
import { ShareGroupDescribeRequest, ShareGroupDescribeResponse, throwIfError } from './common';

/*
ShareGroupDescribe Request (Version: 1) => { (group_ids) include_authorized_operations }
  group_ids => COMPACT_STRING
  include_authorized_operations => BOOLEAN

ShareGroupDescribe Response (Version: 1) => { throttle_time_ms (groups) }
  throttle_time_ms => INT32
  groups => { error_code error_message group_id group_state group_epoch assignment_epoch assignor_name (members) authorized_operations }
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    group_epoch => INT32
    assignment_epoch => INT32
    assignor_name => COMPACT_STRING
    members => { member_id rack_id member_epoch client_id client_host (subscribed_topic_names) assignment }
      member_id => COMPACT_STRING
      rack_id => COMPACT_NULLABLE_STRING
      member_epoch => INT32
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      subscribed_topic_names => COMPACT_STRING
      assignment => { (topic_partitions) }
        topic_partitions => { topic_id topic_name (partitions) }
          topic_id => UUID
          topic_name => COMPACT_STRING
          partitions => INT32
    authorized_operations => INT32
*/
export const SHARE_GROUP_DESCRIBE_V1 = createApi<ShareGroupDescribeRequest, ShareGroupDescribeResponse>({
    apiKey: 77,
    apiVersion: 1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.groupIds, (encoder, groupId) => encoder.writeCompactString(groupId))
            .writeBoolean(data.includeAuthorizedOperations)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            groups: decoder.readCompactArray((group) => ({
                errorCode: group.readInt16(),
                errorMessage: group.readCompactString(),
                groupId: group.readCompactString()!,
                groupState: group.readCompactString()!,
                groupEpoch: group.readInt32(),
                assignmentEpoch: group.readInt32(),
                assignorName: group.readCompactString()!,
                members: group.readCompactArray((member) => ({
                    memberId: member.readCompactString()!,
                    rackId: member.readCompactString(),
                    memberEpoch: member.readInt32(),
                    clientId: member.readCompactString()!,
                    clientHost: member.readCompactString()!,
                    subscribedTopicNames: member.readCompactArray((subscribedTopicName) =>
                        subscribedTopicName.readCompactString()!,
                    ),
                    assignment: member.readStruct((assignment) => ({
                        topicPartitions: assignment.readCompactArray((topicPartition) => ({
                            topicId: topicPartition.readUUID(),
                            topicName: topicPartition.readCompactString()!,
                            partitions: topicPartition.readCompactArray((partition) => partition.readInt32()),
                            tags: topicPartition.readTagBuffer(),
                        })),
                        tags: assignment.readTagBuffer(),
                    })),
                    tags: member.readTagBuffer(),
                })),
                authorizedOperations: group.readInt32(),
                tags: group.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
