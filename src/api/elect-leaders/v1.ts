import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { ELECT_LEADERS_V0 } from './v0';

/*
ElectLeaders Request (Version: 1) => { election_type ?[topic_partitions] timeout_ms }
  election_type => INT8
  topic_partitions => { topic [partitions] }
    topic => STRING
    partitions => INT32
  timeout_ms => INT32

ElectLeaders Response (Version: 1) => { throttle_time_ms error_code [replica_election_results] }
  throttle_time_ms => INT32
  error_code => INT16
  replica_election_results => { topic [partition_result] }
    topic => STRING
    partition_result => { partition_id error_code error_message }
      partition_id => INT32
      error_code => INT16
      error_message => NULLABLE_STRING
*/
export const ELECT_LEADERS_V1 = createApi({
    ...ELECT_LEADERS_V0,
    apiVersion: 1,
    fallback: ELECT_LEADERS_V0,
    request: (encoder, data) =>
        encoder
            .writeInt8(data.electionType ?? 0)
            .writeArray(data.topicPartitions, (encoder, topicPartition) =>
                encoder
                    .writeString(topicPartition.topic)
                    .writeArray(topicPartition.partitions, (encoder, partition) => encoder.writeInt32(partition)),
            )
            .writeInt32(data.timeoutMs),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            replicaElectionResults: decoder.readArray((replicaElectionResult) => ({
                topic: replicaElectionResult.readString()!,
                partitionResult: replicaElectionResult.readArray((partitionResult) => ({
                    partitionId: partitionResult.readInt32(),
                    errorCode: partitionResult.readInt16(),
                    errorMessage: partitionResult.readString(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
