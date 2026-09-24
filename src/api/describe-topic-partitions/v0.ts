import { createApi } from '../../utils/api';
import { DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, throwIfError } from './common';

/*
DescribeTopicPartitions Request (Version: 0) => { (topics) response_partition_limit cursor }
  topics => { name }
    name => COMPACT_STRING
  response_partition_limit => INT32
  cursor => ?{ topic_name partition_index }
    topic_name => COMPACT_STRING
    partition_index => INT32

DescribeTopicPartitions Response (Version: 0) => { throttle_time_ms (topics) next_cursor }
  throttle_time_ms => INT32
  topics => { error_code name topic_id is_internal (partitions) topic_authorized_operations }
    error_code => INT16
    name => COMPACT_NULLABLE_STRING
    topic_id => UUID
    is_internal => BOOLEAN
    partitions => { error_code partition_index leader_id leader_epoch (replica_nodes) (isr_nodes) ?(eligible_leader_replicas) ?(last_known_elr) (offline_replicas) }
      error_code => INT16
      partition_index => INT32
      leader_id => INT32
      leader_epoch => INT32
      replica_nodes => INT32
      isr_nodes => INT32
      eligible_leader_replicas => INT32
      last_known_elr => INT32
      offline_replicas => INT32
    topic_authorized_operations => INT32
  next_cursor => ?{ topic_name partition_index }
    topic_name => COMPACT_STRING
    partition_index => INT32
*/
export const DESCRIBE_TOPIC_PARTITIONS_V0 = createApi<DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse>({
    apiKey: 75,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) => encoder.writeCompactString(topic.name).writeTagBuffer())
            .writeInt32(data.responsePartitionLimit)
            .writeNullableStruct(data.cursor, (encoder, cursor) =>
                encoder.writeCompactString(cursor.topicName).writeInt32(cursor.partitionIndex).writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                errorCode: topic.readInt16(),
                name: topic.readCompactString(),
                topicId: topic.readUUID(),
                isInternal: topic.readBoolean(),
                partitions: topic.readCompactArray((partition) => ({
                    errorCode: partition.readInt16(),
                    partitionIndex: partition.readInt32(),
                    leaderId: partition.readInt32(),
                    leaderEpoch: partition.readInt32(),
                    replicaNodes: partition.readCompactArray((replicaNode) => replicaNode.readInt32()),
                    isrNodes: partition.readCompactArray((isrNode) => isrNode.readInt32()),
                    eligibleLeaderReplicas: partition.readCompactArray((eligibleLeaderReplica) =>
                        eligibleLeaderReplica.readInt32(),
                    ),
                    lastKnownElr: partition.readCompactArray((lastKnownElr) => lastKnownElr.readInt32()),
                    offlineReplicas: partition.readCompactArray((offlineReplica) => offlineReplica.readInt32()),
                    tags: partition.readTagBuffer(),
                })),
                topicAuthorizedOperations: topic.readInt32(),
                tags: topic.readTagBuffer(),
            })),
            nextCursor: decoder.readNullableStruct((nextCursor) => ({
                topicName: nextCursor.readCompactString()!,
                partitionIndex: nextCursor.readInt32(),
                tags: nextCursor.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
