import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { ALTER_PARTITION_REASSIGNMENTS_V0 } from './v0';

/*
AlterPartitionReassignments Request (Version: 1) => { timeout_ms allow_replication_factor_change (topics) }
  timeout_ms => INT32
  allow_replication_factor_change => BOOLEAN
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index ?(replicas) }
      partition_index => INT32
      replicas => INT32

AlterPartitionReassignments Response (Version: 1) => { throttle_time_ms allow_replication_factor_change error_code error_message (responses) }
  throttle_time_ms => INT32
  allow_replication_factor_change => BOOLEAN
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  responses => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code error_message }
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export const ALTER_PARTITION_REASSIGNMENTS_V1 = createApi({
    ...ALTER_PARTITION_REASSIGNMENTS_V0,
    apiVersion: 1,
    fallback: ALTER_PARTITION_REASSIGNMENTS_V0,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.timeoutMs)
            .writeBoolean(data.allowReplicationFactorChange ?? true)
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
            allowReplicationFactorChange: decoder.readBoolean(),
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
