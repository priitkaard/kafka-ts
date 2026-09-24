import { createApi } from '../../utils/api';
import { ElectLeadersRequest, ElectLeadersResponse, throwIfError } from './common';

/*
ElectLeaders Request (Version: 0) => { ?[topic_partitions] timeout_ms }
  topic_partitions => { topic [partitions] }
    topic => STRING
    partitions => INT32
  timeout_ms => INT32

ElectLeaders Response (Version: 0) => { throttle_time_ms [replica_election_results] }
  throttle_time_ms => INT32
  replica_election_results => { topic [partition_result] }
    topic => STRING
    partition_result => { partition_id error_code error_message }
      partition_id => INT32
      error_code => INT16
      error_message => NULLABLE_STRING
*/
export const ELECT_LEADERS_V0 = createApi<ElectLeadersRequest, ElectLeadersResponse>({
    apiKey: 43,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.topicPartitions, (encoder, topicPartition) =>
                encoder
                    .writeString(topicPartition.topic)
                    .writeArray(topicPartition.partitions, (encoder, partition) => encoder.writeInt32(partition)),
            )
            .writeInt32(data.timeoutMs),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: 0,
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
