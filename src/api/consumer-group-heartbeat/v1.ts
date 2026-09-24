import { createApi } from '../../utils/api';
import { CONSUMER_GROUP_HEARTBEAT_V0 } from './v0';

/*
ConsumerGroupHeartbeat Request (Version: 1) => { group_id member_id member_epoch instance_id rack_id rebalance_timeout_ms ?(subscribed_topic_names) subscribed_topic_regex server_assignor ?(topic_partitions) }
  group_id => COMPACT_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  instance_id => COMPACT_NULLABLE_STRING
  rack_id => COMPACT_NULLABLE_STRING
  rebalance_timeout_ms => INT32
  subscribed_topic_names => COMPACT_STRING
  subscribed_topic_regex => COMPACT_NULLABLE_STRING
  server_assignor => COMPACT_NULLABLE_STRING
  topic_partitions => { topic_id (partitions) }
    topic_id => UUID
    partitions => INT32

ConsumerGroupHeartbeat Response (Version: 1) => { throttle_time_ms error_code error_message member_id member_epoch heartbeat_interval_ms assignment }
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
export const CONSUMER_GROUP_HEARTBEAT_V1 = createApi({
    ...CONSUMER_GROUP_HEARTBEAT_V0,
    apiVersion: 1,
    fallback: CONSUMER_GROUP_HEARTBEAT_V0,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactString(data.memberId)
            .writeInt32(data.memberEpoch)
            .writeCompactString(data.instanceId)
            .writeCompactString(data.rackId)
            .writeInt32(data.rebalanceTimeoutMs)
            .writeCompactArray(data.subscribedTopicNames, (encoder, subscribedTopicName) =>
                encoder.writeCompactString(subscribedTopicName),
            )
            .writeCompactString(data.subscribedTopicRegex ?? null)
            .writeCompactString(data.serverAssignor)
            .writeCompactArray(data.topicPartitions, (encoder, topicPartition) =>
                encoder
                    .writeUUID(topicPartition.topicId)
                    .writeCompactArray(topicPartition.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
});
