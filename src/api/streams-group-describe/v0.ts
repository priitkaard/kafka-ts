import { createApi } from '../../utils/api';
import { StreamsGroupDescribeRequest, StreamsGroupDescribeResponse, throwIfError } from './common';

/*
StreamsGroupDescribe Request (Version: 0) => { (group_ids) include_authorized_operations }
  group_ids => COMPACT_STRING
  include_authorized_operations => BOOLEAN

StreamsGroupDescribe Response (Version: 0) => { throttle_time_ms (groups) }
  throttle_time_ms => INT32
  groups => { error_code error_message group_id group_state group_epoch assignment_epoch topology (members) authorized_operations }
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    group_id => COMPACT_STRING
    group_state => COMPACT_STRING
    group_epoch => INT32
    assignment_epoch => INT32
    topology => ?{ epoch ?(subtopologies) }
      epoch => INT32
      subtopologies => { subtopology_id (source_topics) (repartition_sink_topics) (state_changelog_topics) (repartition_source_topics) }
        subtopology_id => COMPACT_STRING
        source_topics => COMPACT_STRING
        repartition_sink_topics => COMPACT_STRING
        state_changelog_topics => { name partitions replication_factor (topic_configs) }
          name => COMPACT_STRING
          partitions => INT32
          replication_factor => INT16
          topic_configs => { key value }
            key => COMPACT_STRING
            value => COMPACT_STRING
        repartition_source_topics => { name partitions replication_factor (topic_configs) }
          name => COMPACT_STRING
          partitions => INT32
          replication_factor => INT16
          topic_configs => { key value }
            key => COMPACT_STRING
            value => COMPACT_STRING
    members => { member_id member_epoch instance_id rack_id client_id client_host topology_epoch process_id user_endpoint (client_tags) (task_offsets) (task_end_offsets) assignment target_assignment is_classic }
      member_id => COMPACT_STRING
      member_epoch => INT32
      instance_id => COMPACT_NULLABLE_STRING
      rack_id => COMPACT_NULLABLE_STRING
      client_id => COMPACT_STRING
      client_host => COMPACT_STRING
      topology_epoch => INT32
      process_id => COMPACT_STRING
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
      assignment => { (active_tasks) (standby_tasks) (warmup_tasks) }
        active_tasks => { subtopology_id (partitions) }
          subtopology_id => COMPACT_STRING
          partitions => INT32
        standby_tasks => { subtopology_id (partitions) }
          subtopology_id => COMPACT_STRING
          partitions => INT32
        warmup_tasks => { subtopology_id (partitions) }
          subtopology_id => COMPACT_STRING
          partitions => INT32
      target_assignment => { (active_tasks) (standby_tasks) (warmup_tasks) }
        active_tasks => { subtopology_id (partitions) }
          subtopology_id => COMPACT_STRING
          partitions => INT32
        standby_tasks => { subtopology_id (partitions) }
          subtopology_id => COMPACT_STRING
          partitions => INT32
        warmup_tasks => { subtopology_id (partitions) }
          subtopology_id => COMPACT_STRING
          partitions => INT32
      is_classic => BOOLEAN
    authorized_operations => INT32
*/
export const STREAMS_GROUP_DESCRIBE_V0 = createApi<StreamsGroupDescribeRequest, StreamsGroupDescribeResponse>({
    apiKey: 89,
    apiVersion: 0,
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
                topology: group.readNullableStruct((topology) => ({
                    epoch: topology.readInt32(),
                    subtopologies: topology.readCompactArray((subtopology) => ({
                        subtopologyId: subtopology.readCompactString()!,
                        sourceTopics: subtopology.readCompactArray((sourceTopic) => sourceTopic.readCompactString()!),
                        repartitionSinkTopics: subtopology.readCompactArray((repartitionSinkTopic) =>
                            repartitionSinkTopic.readCompactString()!,
                        ),
                        stateChangelogTopics: subtopology.readCompactArray((stateChangelogTopic) => ({
                            name: stateChangelogTopic.readCompactString()!,
                            partitions: stateChangelogTopic.readInt32(),
                            replicationFactor: stateChangelogTopic.readInt16(),
                            topicConfigs: stateChangelogTopic.readCompactArray((topicConfig) => ({
                                key: topicConfig.readCompactString()!,
                                value: topicConfig.readCompactString()!,
                                tags: topicConfig.readTagBuffer(),
                            })),
                            tags: stateChangelogTopic.readTagBuffer(),
                        })),
                        repartitionSourceTopics: subtopology.readCompactArray((repartitionSourceTopic) => ({
                            name: repartitionSourceTopic.readCompactString()!,
                            partitions: repartitionSourceTopic.readInt32(),
                            replicationFactor: repartitionSourceTopic.readInt16(),
                            topicConfigs: repartitionSourceTopic.readCompactArray((topicConfig) => ({
                                key: topicConfig.readCompactString()!,
                                value: topicConfig.readCompactString()!,
                                tags: topicConfig.readTagBuffer(),
                            })),
                            tags: repartitionSourceTopic.readTagBuffer(),
                        })),
                        tags: subtopology.readTagBuffer(),
                    })),
                    tags: topology.readTagBuffer(),
                })),
                members: group.readCompactArray((member) => ({
                    memberId: member.readCompactString()!,
                    memberEpoch: member.readInt32(),
                    instanceId: member.readCompactString(),
                    rackId: member.readCompactString(),
                    clientId: member.readCompactString()!,
                    clientHost: member.readCompactString()!,
                    topologyEpoch: member.readInt32(),
                    processId: member.readCompactString()!,
                    userEndpoint: member.readNullableStruct((userEndpoint) => ({
                        host: userEndpoint.readCompactString()!,
                        port: userEndpoint.readUInt16(),
                        tags: userEndpoint.readTagBuffer(),
                    })),
                    clientTags: member.readCompactArray((clientTag) => ({
                        key: clientTag.readCompactString()!,
                        value: clientTag.readCompactString()!,
                        tags: clientTag.readTagBuffer(),
                    })),
                    taskOffsets: member.readCompactArray((taskOffset) => ({
                        subtopologyId: taskOffset.readCompactString()!,
                        partition: taskOffset.readInt32(),
                        offset: taskOffset.readInt64(),
                        tags: taskOffset.readTagBuffer(),
                    })),
                    taskEndOffsets: member.readCompactArray((taskEndOffset) => ({
                        subtopologyId: taskEndOffset.readCompactString()!,
                        partition: taskEndOffset.readInt32(),
                        offset: taskEndOffset.readInt64(),
                        tags: taskEndOffset.readTagBuffer(),
                    })),
                    assignment: member.readStruct((assignment) => ({
                        activeTasks: assignment.readCompactArray((activeTask) => ({
                            subtopologyId: activeTask.readCompactString()!,
                            partitions: activeTask.readCompactArray((partition) => partition.readInt32()),
                            tags: activeTask.readTagBuffer(),
                        })),
                        standbyTasks: assignment.readCompactArray((standbyTask) => ({
                            subtopologyId: standbyTask.readCompactString()!,
                            partitions: standbyTask.readCompactArray((partition) => partition.readInt32()),
                            tags: standbyTask.readTagBuffer(),
                        })),
                        warmupTasks: assignment.readCompactArray((warmupTask) => ({
                            subtopologyId: warmupTask.readCompactString()!,
                            partitions: warmupTask.readCompactArray((partition) => partition.readInt32()),
                            tags: warmupTask.readTagBuffer(),
                        })),
                        tags: assignment.readTagBuffer(),
                    })),
                    targetAssignment: member.readStruct((targetAssignment) => ({
                        activeTasks: targetAssignment.readCompactArray((activeTask) => ({
                            subtopologyId: activeTask.readCompactString()!,
                            partitions: activeTask.readCompactArray((partition) => partition.readInt32()),
                            tags: activeTask.readTagBuffer(),
                        })),
                        standbyTasks: targetAssignment.readCompactArray((standbyTask) => ({
                            subtopologyId: standbyTask.readCompactString()!,
                            partitions: standbyTask.readCompactArray((partition) => partition.readInt32()),
                            tags: standbyTask.readTagBuffer(),
                        })),
                        warmupTasks: targetAssignment.readCompactArray((warmupTask) => ({
                            subtopologyId: warmupTask.readCompactString()!,
                            partitions: warmupTask.readCompactArray((partition) => partition.readInt32()),
                            tags: warmupTask.readTagBuffer(),
                        })),
                        tags: targetAssignment.readTagBuffer(),
                    })),
                    isClassic: member.readBoolean(),
                    tags: member.readTagBuffer(),
                })),
                authorizedOperations: group.readInt32(),
                tags: group.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
