import { createApi } from '../../utils/api';
import { OffsetDeleteRequest, OffsetDeleteResponse, throwIfError } from './common';

/*
OffsetDelete Request (Version: 0) => { group_id [topics] }
  group_id => STRING
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index }
      partition_index => INT32

OffsetDelete Response (Version: 0) => { error_code throttle_time_ms [topics] }
  error_code => INT16
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const OFFSET_DELETE_V0 = createApi<OffsetDeleteRequest, OffsetDeleteResponse>({
    apiKey: 47,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitions, (encoder, partition) => encoder.writeInt32(partition.partitionIndex)),
            ),
    response: (decoder) =>
        throwIfError({
            errorCode: decoder.readInt16(),
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readArray((topic) => ({
                name: topic.readString()!,
                partitions: topic.readArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
