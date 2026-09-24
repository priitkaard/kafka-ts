import { createApi } from '../../utils/api';
import { ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse, throwIfError } from './common';

/*
ShareGroupHeartbeat Request (Version: 1) => { group_id member_id member_epoch rack_id ?(subscribed_topic_names) }
  group_id => COMPACT_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  rack_id => COMPACT_NULLABLE_STRING
  subscribed_topic_names => COMPACT_STRING

ShareGroupHeartbeat Response (Version: 1) => { throttle_time_ms error_code error_message member_id member_epoch heartbeat_interval_ms assignment }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  member_epoch => INT32
  heartbeat_interval_ms => INT32
  assignment => ?{ (topic_partitions) }
    topic_partitions => { topic_id (partitions) }
      topic_id => UUID
      partitions => INT32
*/
export const SHARE_GROUP_HEARTBEAT_V1 = createApi<ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse>({
    apiKey: 76,
    apiVersion: 1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactString(data.memberId)
            .writeInt32(data.memberEpoch)
            .writeCompactString(data.rackId)
            .writeCompactArray(data.subscribedTopicNames, (encoder, subscribedTopicName) =>
                encoder.writeCompactString(subscribedTopicName),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            memberId: decoder.readCompactString(),
            memberEpoch: decoder.readInt32(),
            heartbeatIntervalMs: decoder.readInt32(),
            assignment: decoder.readNullableStruct((assignment) => ({
                topicPartitions: assignment.readCompactArray((topicPartition) => ({
                    topicId: topicPartition.readUUID(),
                    partitions: topicPartition.readCompactArray((partition) => partition.readInt32()),
                    tags: topicPartition.readTagBuffer(),
                })),
                tags: assignment.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
