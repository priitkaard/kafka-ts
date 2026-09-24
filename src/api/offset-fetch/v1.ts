import { createApi } from '../../utils/api';
import { getSingleGroup, OffsetFetchRequest, OffsetFetchResponse, throwIfError } from './common';

/*
OffsetFetch Request (Version: 1) => { group_id [topics] }
  group_id => STRING
  topics => { name [partition_indexes] }
    name => STRING
    partition_indexes => INT32

OffsetFetch Response (Version: 1) => { [topics] }
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset metadata error_code }
      partition_index => INT32
      committed_offset => INT64
      metadata => NULLABLE_STRING
      error_code => INT16
*/
export const OFFSET_FETCH_V1 = createApi<OffsetFetchRequest, OffsetFetchResponse>({
    apiKey: 9,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) => {
        const group = getSingleGroup(data);
        return encoder
            .writeString(group.groupId)
            .writeArray(group.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                        encoder.writeInt32(partitionIndex),
                    ),
            );
    },
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
            groups: [
                {
                    groupId: '',
                    topics: decoder.readArray((topic) => ({
                        name: topic.readString()!,
                        partitions: topic.readArray((partition) => ({
                            partitionIndex: partition.readInt32(),
                            committedOffset: partition.readInt64(),
                            committedLeaderEpoch: -1,
                            committedMetadata: partition.readString(),
                            errorCode: partition.readInt16(),
                            tags: {},
                        })),
                        tags: {},
                    })),
                    errorCode: 0,
                    tags: {},
                },
            ],
            tags: {},
        }),
});
