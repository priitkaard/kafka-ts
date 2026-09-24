import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { LIST_OFFSETS_V1 } from './v1';

/*
ListOffsets Request (Version: 2) => { replica_id isolation_level [topics] }
  replica_id => INT32
  isolation_level => INT8
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index timestamp }
      partition_index => INT32
      timestamp => INT64

ListOffsets Response (Version: 2) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code timestamp offset }
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
*/
export const LIST_OFFSETS_V2 = createApi({
    ...LIST_OFFSETS_V1,
    apiVersion: 2,
    fallback: LIST_OFFSETS_V1,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.replicaId)
            .writeInt8(data.isolationLevel)
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition.partitionIndex).writeInt64(partition.timestamp),
                    ),
            ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readArray((topic) => ({
                name: topic.readString()!,
                partitions: topic.readArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    timestamp: partition.readInt64(),
                    offset: partition.readInt64(),
                    leaderEpoch: -1,
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
