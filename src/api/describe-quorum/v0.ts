import { createApi } from '../../utils/api';
import { DescribeQuorumRequest, DescribeQuorumResponse, throwIfError } from './common';

/*
DescribeQuorum Request (Version: 0) => { (topics) }
  topics => { topic_name (partitions) }
    topic_name => COMPACT_STRING
    partitions => { partition_index }
      partition_index => INT32

DescribeQuorum Response (Version: 0) => { error_code (topics) }
  error_code => INT16
  topics => { topic_name (partitions) }
    topic_name => COMPACT_STRING
    partitions => { partition_index error_code leader_id leader_epoch high_watermark (current_voters) (observers) }
      partition_index => INT32
      error_code => INT16
      leader_id => INT32
      leader_epoch => INT32
      high_watermark => INT64
      current_voters => { replica_id log_end_offset }
        replica_id => INT32
        log_end_offset => INT64
      observers => { replica_id log_end_offset }
        replica_id => INT32
        log_end_offset => INT64
*/
export const DESCRIBE_QUORUM_V0 = createApi<DescribeQuorumRequest, DescribeQuorumResponse>({
    apiKey: 55,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.topicName)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition.partitionIndex).writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
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
                        lastFetchTimestamp: 0n,
                        lastCaughtUpTimestamp: 0n,
                        tags: currentVoter.readTagBuffer(),
                    })),
                    observers: partition.readCompactArray((observer) => ({
                        replicaId: observer.readInt32(),
                        replicaDirectoryId: '',
                        logEndOffset: observer.readInt64(),
                        lastFetchTimestamp: 0n,
                        lastCaughtUpTimestamp: 0n,
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
