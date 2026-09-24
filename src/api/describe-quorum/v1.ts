import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_QUORUM_V0 } from './v0';

/*
DescribeQuorum Request (Version: 1) => { (topics) }
  topics => { topic_name (partitions) }
    topic_name => COMPACT_STRING
    partitions => { partition_index }
      partition_index => INT32

DescribeQuorum Response (Version: 1) => { error_code (topics) }
  error_code => INT16
  topics => { topic_name (partitions) }
    topic_name => COMPACT_STRING
    partitions => { partition_index error_code leader_id leader_epoch high_watermark (current_voters) (observers) }
      partition_index => INT32
      error_code => INT16
      leader_id => INT32
      leader_epoch => INT32
      high_watermark => INT64
      current_voters => { replica_id log_end_offset last_fetch_timestamp last_caught_up_timestamp }
        replica_id => INT32
        log_end_offset => INT64
        last_fetch_timestamp => INT64
        last_caught_up_timestamp => INT64
      observers => { replica_id log_end_offset last_fetch_timestamp last_caught_up_timestamp }
        replica_id => INT32
        log_end_offset => INT64
        last_fetch_timestamp => INT64
        last_caught_up_timestamp => INT64
*/
export const DESCRIBE_QUORUM_V1 = createApi({
    ...DESCRIBE_QUORUM_V0,
    apiVersion: 1,
    fallback: DESCRIBE_QUORUM_V0,
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            errorMessage: '',
            topics: decoder.readCompactArray((topic) => ({
                topicName: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    errorMessage: '',
                    leaderId: partition.readInt32(),
                    leaderEpoch: partition.readInt32(),
                    highWatermark: partition.readInt64(),
                    currentVoters: partition.readCompactArray((currentVoter) => ({
                        replicaId: currentVoter.readInt32(),
                        replicaDirectoryId: '',
                        logEndOffset: currentVoter.readInt64(),
                        lastFetchTimestamp: currentVoter.readInt64(),
                        lastCaughtUpTimestamp: currentVoter.readInt64(),
                        tags: currentVoter.readTagBuffer(),
                    })),
                    observers: partition.readCompactArray((observer) => ({
                        replicaId: observer.readInt32(),
                        replicaDirectoryId: '',
                        logEndOffset: observer.readInt64(),
                        lastFetchTimestamp: observer.readInt64(),
                        lastCaughtUpTimestamp: observer.readInt64(),
                        tags: observer.readTagBuffer(),
                    })),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            nodes: [],
            tags: decoder.readTagBuffer(),
        }),
});
