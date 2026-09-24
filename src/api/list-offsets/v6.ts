import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LIST_OFFSETS_V5 } from './v5';

/*
ListOffsets Request (Version: 6) => { replica_id isolation_level (topics) }
  replica_id => INT32
  isolation_level => INT8
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index current_leader_epoch timestamp }
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64

ListOffsets Response (Version: 6) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code timestamp offset leader_epoch }
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
*/
export const LIST_OFFSETS_V6 = createApi({
    ...LIST_OFFSETS_V5,
    apiVersion: 6,
    fallback: LIST_OFFSETS_V5,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.replicaId)
            .writeInt8(data.isolationLevel)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt32(partition.currentLeaderEpoch)
                            .writeInt64(partition.timestamp)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                name: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    timestamp: partition.readInt64(),
                    offset: partition.readInt64(),
                    leaderEpoch: partition.readInt32(),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
