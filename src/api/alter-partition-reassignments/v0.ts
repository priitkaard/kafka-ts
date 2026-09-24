import { createApi } from '../../utils/api';
import { AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, throwIfError } from './common';

/*
AlterPartitionReassignments Request (Version: 0) => { timeout_ms (topics) }
  timeout_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index ?(replicas) }
      partition_index => INT32
      replicas => INT32

AlterPartitionReassignments Response (Version: 0) => { throttle_time_ms error_code error_message (responses) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code error_message }
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export const ALTER_PARTITION_REASSIGNMENTS_V0 = createApi<
    AlterPartitionReassignmentsRequest,
    AlterPartitionReassignmentsResponse
>({
    apiKey: 45,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.timeoutMs)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeCompactArray(partition.replicas, (encoder, replica) => encoder.writeInt32(replica))
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            allowReplicationFactorChange: true,
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            responses: decoder.readCompactArray((response) => ({
                name: response.readCompactString()!,
                partitions: response.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    errorMessage: partition.readCompactString(),
                    tags: partition.readTagBuffer(),
                })),
                tags: response.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
