import { createApi } from '../../utils/api';
import { ListOffsetsRequest, ListOffsetsResponse, throwIfError } from './common';

/*
ListOffsets Request (Version: 1) => { replica_id [topics] }
  replica_id => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index timestamp }
      partition_index => INT32
      timestamp => INT64

ListOffsets Response (Version: 1) => { [topics] }
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code timestamp offset }
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
*/
export const LIST_OFFSETS_V1 = createApi<ListOffsetsRequest, ListOffsetsResponse>({
    apiKey: 2,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.replicaId)
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition.partitionIndex).writeInt64(partition.timestamp),
                    ),
            ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
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
