import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { ELECT_LEADERS_V1 } from './v1';

/*
ElectLeaders Request (Version: 2) => { election_type ?(topic_partitions) timeout_ms }
  election_type => INT8
  topic_partitions => { topic (partitions) }
    topic => COMPACT_STRING
    partitions => INT32
  timeout_ms => INT32

ElectLeaders Response (Version: 2) => { throttle_time_ms error_code (replica_election_results) }
  throttle_time_ms => INT32
  error_code => INT16
  replica_election_results => { topic (partition_result) }
    topic => COMPACT_STRING
    partition_result => { partition_id error_code error_message }
      partition_id => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export const ELECT_LEADERS_V2 = createApi({
    ...ELECT_LEADERS_V1,
    apiVersion: 2,
    fallback: ELECT_LEADERS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt8(data.electionType ?? 0)
            .writeCompactArray(data.topicPartitions, (encoder, topicPartition) =>
                encoder
                    .writeCompactString(topicPartition.topic)
                    .writeCompactArray(topicPartition.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeTagBuffer(),
            )
            .writeInt32(data.timeoutMs)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            replicaElectionResults: decoder.readCompactArray((replicaElectionResult) => ({
                topic: replicaElectionResult.readCompactString()!,
                partitionResult: replicaElectionResult.readCompactArray((partitionResult) => ({
                    partitionId: partitionResult.readInt32(),
                    errorCode: partitionResult.readInt16(),
                    errorMessage: partitionResult.readCompactString(),
                    tags: partitionResult.readTagBuffer(),
                })),
                tags: replicaElectionResult.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
