import { createApi } from '../../utils/api';
import { ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, throwIfError } from './common';

/*
ListPartitionReassignments Request (Version: 0) => { timeout_ms ?(topics) }
  timeout_ms => INT32
  topics => { name (partition_indexes) }
    name => COMPACT_STRING
    partition_indexes => INT32

ListPartitionReassignments Response (Version: 0) => { throttle_time_ms error_code error_message (topics) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index (replicas) (adding_replicas) (removing_replicas) }
      partition_index => INT32
      replicas => INT32
      adding_replicas => INT32
      removing_replicas => INT32
*/
export const LIST_PARTITION_REASSIGNMENTS_V0 = createApi<
    ListPartitionReassignmentsRequest,
    ListPartitionReassignmentsResponse
>({
    apiKey: 46,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.timeoutMs)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                        encoder.writeInt32(partitionIndex),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            topics: decoder.readCompactArray((topic) => ({
                name: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    replicas: partition.readCompactArray((replica) => replica.readInt32()),
                    addingReplicas: partition.readCompactArray((addingReplica) => addingReplica.readInt32()),
                    removingReplicas: partition.readCompactArray((removingReplica) => removingReplica.readInt32()),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
