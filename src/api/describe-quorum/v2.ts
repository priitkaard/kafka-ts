import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_QUORUM_V1 } from './v1';

/*
DescribeQuorum Request (Version: 2) => { (topics) }
  topics => { topic_name (partitions) }
    topic_name => COMPACT_STRING
    partitions => { partition_index }
      partition_index => INT32

DescribeQuorum Response (Version: 2) => { error_code error_message (topics) (nodes) }
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  topics => { topic_name (partitions) }
    topic_name => COMPACT_STRING
    partitions => { partition_index error_code error_message leader_id leader_epoch high_watermark (current_voters) (observers) }
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      leader_id => INT32
      leader_epoch => INT32
      high_watermark => INT64
      current_voters => { replica_id replica_directory_id log_end_offset last_fetch_timestamp last_caught_up_timestamp }
        replica_id => INT32
        replica_directory_id => UUID
        log_end_offset => INT64
        last_fetch_timestamp => INT64
        last_caught_up_timestamp => INT64
      observers => { replica_id replica_directory_id log_end_offset last_fetch_timestamp last_caught_up_timestamp }
        replica_id => INT32
        replica_directory_id => UUID
        log_end_offset => INT64
        last_fetch_timestamp => INT64
        last_caught_up_timestamp => INT64
  nodes => { node_id (listeners) }
    node_id => INT32
    listeners => { name host port }
      name => COMPACT_STRING
      host => COMPACT_STRING
      port => UINT16
*/
export const DESCRIBE_QUORUM_V2 = createApi({
    ...DESCRIBE_QUORUM_V1,
    apiVersion: 2,
    fallback: DESCRIBE_QUORUM_V1,
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            topics: decoder.readCompactArray((topic) => ({
                topicName: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    errorMessage: partition.readCompactString(),
                    leaderId: partition.readInt32(),
                    leaderEpoch: partition.readInt32(),
                    highWatermark: partition.readInt64(),
                    currentVoters: partition.readCompactArray((currentVoter) => ({
                        replicaId: currentVoter.readInt32(),
                        replicaDirectoryId: currentVoter.readUUID(),
                        logEndOffset: currentVoter.readInt64(),
                        lastFetchTimestamp: currentVoter.readInt64(),
                        lastCaughtUpTimestamp: currentVoter.readInt64(),
                        tags: currentVoter.readTagBuffer(),
                    })),
                    observers: partition.readCompactArray((observer) => ({
                        replicaId: observer.readInt32(),
                        replicaDirectoryId: observer.readUUID(),
                        logEndOffset: observer.readInt64(),
                        lastFetchTimestamp: observer.readInt64(),
                        lastCaughtUpTimestamp: observer.readInt64(),
                        tags: observer.readTagBuffer(),
                    })),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            nodes: decoder.readCompactArray((node) => ({
                nodeId: node.readInt32(),
                listeners: node.readCompactArray((listener) => ({
                    name: listener.readCompactString()!,
                    host: listener.readCompactString()!,
                    port: listener.readUInt16(),
                    tags: listener.readTagBuffer(),
                })),
                tags: node.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
