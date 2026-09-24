import { createApi } from '../../utils/api';
import { StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse, throwIfError } from './common';

/*
StreamsGroupHeartbeat Request (Version: 0) => { group_id member_id member_epoch endpoint_information_epoch instance_id rack_id rebalance_timeout_ms topology ?(active_tasks) ?(standby_tasks) ?(warmup_tasks) process_id user_endpoint ?(client_tags) ?(task_offsets) ?(task_end_offsets) shutdown_application }
  group_id => COMPACT_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  endpoint_information_epoch => INT32
  instance_id => COMPACT_NULLABLE_STRING
  rack_id => COMPACT_NULLABLE_STRING
  rebalance_timeout_ms => INT32
  topology => ?{ epoch (subtopologies) }
    epoch => INT32
    subtopologies => { subtopology_id (source_topics) (source_topic_regex) (state_changelog_topics) (repartition_sink_topics) (repartition_source_topics) (copartition_groups) }
      subtopology_id => COMPACT_STRING
      source_topics => COMPACT_STRING
      source_topic_regex => COMPACT_STRING
      state_changelog_topics => { name partitions replication_factor (topic_configs) }
        name => COMPACT_STRING
        partitions => INT32
        replication_factor => INT16
        topic_configs => { key value }
          key => COMPACT_STRING
          value => COMPACT_STRING
      repartition_sink_topics => COMPACT_STRING
      repartition_source_topics => { name partitions replication_factor (topic_configs) }
        name => COMPACT_STRING
        partitions => INT32
        replication_factor => INT16
        topic_configs => { key value }
          key => COMPACT_STRING
          value => COMPACT_STRING
      copartition_groups => { (source_topics) (source_topic_regex) (repartition_source_topics) }
        source_topics => INT16
        source_topic_regex => INT16
        repartition_source_topics => INT16
  active_tasks => { subtopology_id (partitions) }
    subtopology_id => COMPACT_STRING
    partitions => INT32
  standby_tasks => { subtopology_id (partitions) }
    subtopology_id => COMPACT_STRING
    partitions => INT32
  warmup_tasks => { subtopology_id (partitions) }
    subtopology_id => COMPACT_STRING
    partitions => INT32
  process_id => COMPACT_NULLABLE_STRING
  user_endpoint => ?{ host port }
    host => COMPACT_STRING
    port => UINT16
  client_tags => { key value }
    key => COMPACT_STRING
    value => COMPACT_STRING
  task_offsets => { subtopology_id partition offset }
    subtopology_id => COMPACT_STRING
    partition => INT32
    offset => INT64
  task_end_offsets => { subtopology_id partition offset }
    subtopology_id => COMPACT_STRING
    partition => INT32
    offset => INT64
  shutdown_application => BOOLEAN

StreamsGroupHeartbeat Response (Version: 0) => { throttle_time_ms error_code error_message member_id member_epoch heartbeat_interval_ms acceptable_recovery_lag task_offset_interval_ms ?(status) ?(active_tasks) ?(standby_tasks) ?(warmup_tasks) endpoint_information_epoch ?(partitions_by_user_endpoint) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  member_id => COMPACT_STRING
  member_epoch => INT32
  heartbeat_interval_ms => INT32
  acceptable_recovery_lag => INT32
  task_offset_interval_ms => INT32
  status => { status_code status_detail }
    status_code => INT8
    status_detail => COMPACT_STRING
  active_tasks => { subtopology_id (partitions) }
    subtopology_id => COMPACT_STRING
    partitions => INT32
  standby_tasks => { subtopology_id (partitions) }
    subtopology_id => COMPACT_STRING
    partitions => INT32
  warmup_tasks => { subtopology_id (partitions) }
    subtopology_id => COMPACT_STRING
    partitions => INT32
  endpoint_information_epoch => INT32
  partitions_by_user_endpoint => { user_endpoint (active_partitions) (standby_partitions) }
    user_endpoint => { host port }
      host => COMPACT_STRING
      port => UINT16
    active_partitions => { topic (partitions) }
      topic => COMPACT_STRING
      partitions => INT32
    standby_partitions => { topic (partitions) }
      topic => COMPACT_STRING
      partitions => INT32
*/
export const STREAMS_GROUP_HEARTBEAT_V0 = createApi<StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse>({
    apiKey: 88,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactString(data.memberId)
            .writeInt32(data.memberEpoch)
            .writeInt32(data.endpointInformationEpoch)
            .writeCompactString(data.instanceId)
            .writeCompactString(data.rackId)
            .writeInt32(data.rebalanceTimeoutMs)
            .writeNullableStruct(data.topology, (encoder, topology) =>
                encoder
                    .writeInt32(topology.epoch)
                    .writeCompactArray(topology.subtopologies, (encoder, subtopology) =>
                        encoder
                            .writeCompactString(subtopology.subtopologyId)
                            .writeCompactArray(subtopology.sourceTopics, (encoder, sourceTopic) =>
                                encoder.writeCompactString(sourceTopic),
                            )
                            .writeCompactArray(subtopology.sourceTopicRegex, (encoder, sourceTopicRegex) =>
                                encoder.writeCompactString(sourceTopicRegex),
                            )
                            .writeCompactArray(subtopology.stateChangelogTopics, (encoder, stateChangelogTopic) =>
                                encoder
                                    .writeCompactString(stateChangelogTopic.name)
                                    .writeInt32(stateChangelogTopic.partitions)
                                    .writeInt16(stateChangelogTopic.replicationFactor)
                                    .writeCompactArray(stateChangelogTopic.topicConfigs, (encoder, topicConfig) =>
                                        encoder
                                            .writeCompactString(topicConfig.key)
                                            .writeCompactString(topicConfig.value)
                                            .writeTagBuffer(),
                                    )
                                    .writeTagBuffer(),
                            )
                            .writeCompactArray(subtopology.repartitionSinkTopics, (encoder, repartitionSinkTopic) =>
                                encoder.writeCompactString(repartitionSinkTopic),
                            )
                            .writeCompactArray(subtopology.repartitionSourceTopics, (encoder, repartitionSourceTopic) =>
                                encoder
                                    .writeCompactString(repartitionSourceTopic.name)
                                    .writeInt32(repartitionSourceTopic.partitions)
                                    .writeInt16(repartitionSourceTopic.replicationFactor)
                                    .writeCompactArray(repartitionSourceTopic.topicConfigs, (encoder, topicConfig) =>
                                        encoder
                                            .writeCompactString(topicConfig.key)
                                            .writeCompactString(topicConfig.value)
                                            .writeTagBuffer(),
                                    )
                                    .writeTagBuffer(),
                            )
                            .writeCompactArray(subtopology.copartitionGroups, (encoder, copartitionGroup) =>
                                encoder
                                    .writeCompactArray(copartitionGroup.sourceTopics, (encoder, sourceTopic) =>
                                        encoder.writeInt16(sourceTopic),
                                    )
                                    .writeCompactArray(copartitionGroup.sourceTopicRegex, (encoder, sourceTopicRegex) =>
                                        encoder.writeInt16(sourceTopicRegex),
                                    )
                                    .writeCompactArray(
                                        copartitionGroup.repartitionSourceTopics,
                                        (encoder, repartitionSourceTopic) => encoder.writeInt16(repartitionSourceTopic),
                                    )
                                    .writeTagBuffer(),
                            )
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeCompactArray(data.activeTasks, (encoder, activeTask) =>
                encoder
                    .writeCompactString(activeTask.subtopologyId)
                    .writeCompactArray(activeTask.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeTagBuffer(),
            )
            .writeCompactArray(data.standbyTasks, (encoder, standbyTask) =>
                encoder
                    .writeCompactString(standbyTask.subtopologyId)
                    .writeCompactArray(standbyTask.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeTagBuffer(),
            )
            .writeCompactArray(data.warmupTasks, (encoder, warmupTask) =>
                encoder
                    .writeCompactString(warmupTask.subtopologyId)
                    .writeCompactArray(warmupTask.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeTagBuffer(),
            )
            .writeCompactString(data.processId)
            .writeNullableStruct(data.userEndpoint, (encoder, userEndpoint) =>
                encoder.writeCompactString(userEndpoint.host).writeUInt16(userEndpoint.port).writeTagBuffer(),
            )
            .writeCompactArray(data.clientTags, (encoder, clientTag) =>
                encoder.writeCompactString(clientTag.key).writeCompactString(clientTag.value).writeTagBuffer(),
            )
            .writeCompactArray(data.taskOffsets, (encoder, taskOffset) =>
                encoder
                    .writeCompactString(taskOffset.subtopologyId)
                    .writeInt32(taskOffset.partition)
                    .writeInt64(taskOffset.offset)
                    .writeTagBuffer(),
            )
            .writeCompactArray(data.taskEndOffsets, (encoder, taskEndOffset) =>
                encoder
                    .writeCompactString(taskEndOffset.subtopologyId)
                    .writeInt32(taskEndOffset.partition)
                    .writeInt64(taskEndOffset.offset)
                    .writeTagBuffer(),
            )
            .writeBoolean(data.shutdownApplication)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            memberId: decoder.readCompactString()!,
            memberEpoch: decoder.readInt32(),
            heartbeatIntervalMs: decoder.readInt32(),
            acceptableRecoveryLag: decoder.readInt32(),
            taskOffsetIntervalMs: decoder.readInt32(),
            status: decoder.readCompactArray((statu) => ({
                statusCode: statu.readInt8(),
                statusDetail: statu.readCompactString()!,
                tags: statu.readTagBuffer(),
            })),
            activeTasks: decoder.readCompactArray((activeTask) => ({
                subtopologyId: activeTask.readCompactString()!,
                partitions: activeTask.readCompactArray((partition) => partition.readInt32()),
                tags: activeTask.readTagBuffer(),
            })),
            standbyTasks: decoder.readCompactArray((standbyTask) => ({
                subtopologyId: standbyTask.readCompactString()!,
                partitions: standbyTask.readCompactArray((partition) => partition.readInt32()),
                tags: standbyTask.readTagBuffer(),
            })),
            warmupTasks: decoder.readCompactArray((warmupTask) => ({
                subtopologyId: warmupTask.readCompactString()!,
                partitions: warmupTask.readCompactArray((partition) => partition.readInt32()),
                tags: warmupTask.readTagBuffer(),
            })),
            endpointInformationEpoch: decoder.readInt32(),
            partitionsByUserEndpoint: decoder.readCompactArray((partitionsByUserEndpoint) => ({
                userEndpoint: partitionsByUserEndpoint.readStruct((userEndpoint) => ({
                    host: userEndpoint.readCompactString()!,
                    port: userEndpoint.readUInt16(),
                    tags: userEndpoint.readTagBuffer(),
                })),
                activePartitions: partitionsByUserEndpoint.readCompactArray((activePartition) => ({
                    topic: activePartition.readCompactString()!,
                    partitions: activePartition.readCompactArray((partition) => partition.readInt32()),
                    tags: activePartition.readTagBuffer(),
                })),
                standbyPartitions: partitionsByUserEndpoint.readCompactArray((standbyPartition) => ({
                    topic: standbyPartition.readCompactString()!,
                    partitions: standbyPartition.readCompactArray((partition) => partition.readInt32()),
                    tags: standbyPartition.readTagBuffer(),
                })),
                tags: partitionsByUserEndpoint.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
