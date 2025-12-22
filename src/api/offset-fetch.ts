import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type OffsetFetchRequest = {
    groups: {
        groupId: string;
        topics: {
            name: string;
            partitionIndexes: number[];
        }[];
    }[];
    requireStable: boolean;
};

type OffsetFetchResponse = {
    throttleTimeMs: number;
    groups: {
        groupId: string;
        topics: {
            name: string;
            partitions: {
                partitionIndex: number;
                committedOffset: bigint;
                committedLeaderEpoch: number;
                committedMetadata: string | null;
                errorCode: number;
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        }[];
        errorCode: number;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

/*
OffsetFetch Request (Version: 1) => group_id [topics] 
  group_id => STRING
  topics => name [partition_indexes] 
    name => STRING
    partition_indexes => INT32

OffsetFetch Response (Version: 1) => [topics] 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset metadata error_code 
      partition_index => INT32
      committed_offset => INT64
      metadata => NULLABLE_STRING
      error_code => INT16
*/
const OFFSET_FETCH_V1 = createApi<OffsetFetchRequest, OffsetFetchResponse>({
    apiKey: 9,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groups[0].groupId)
            .writeArray(data.groups[0].topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                        encoder.writeInt32(partitionIndex),
                    ),
            ),
    response: (decoder) => {
        const result = {
            throttleTimeMs: 0,
            groups: [
                {
                    groupId: '', // Not provided in v1 response
                    topics: decoder.readArray((decoder) => ({
                        name: decoder.readString()!,
                        partitions: decoder.readArray((decoder) => ({
                            partitionIndex: decoder.readInt32(),
                            committedOffset: decoder.readInt64(),
                            committedLeaderEpoch: -1,
                            committedMetadata: decoder.readString(),
                            errorCode: decoder.readInt16(),
                            tags: {},
                        })),
                        tags: {},
                    })),
                    errorCode: 0,
                    tags: {},
                },
            ],
            tags: {},
        };
        result.groups.forEach((group) => {
            group.topics.forEach((topic) => {
                topic.partitions.forEach((partition) => {
                    if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
                });
            });
        });
        return result;
    },
});

/*
OffsetFetch Request (Version: 8) => [groups] require_stable _tagged_fields 
  groups => group_id [topics] _tagged_fields 
    group_id => COMPACT_STRING
    topics => name [partition_indexes] _tagged_fields 
      name => COMPACT_STRING
      partition_indexes => INT32
  require_stable => BOOLEAN

OffsetFetch Response (Version: 8) => throttle_time_ms [groups] _tagged_fields 
  throttle_time_ms => INT32
  groups => group_id [topics] error_code _tagged_fields 
    group_id => COMPACT_STRING
    topics => name [partitions] _tagged_fields 
      name => COMPACT_STRING
      partitions => partition_index committed_offset committed_leader_epoch metadata error_code _tagged_fields 
        partition_index => INT32
        committed_offset => INT64
        committed_leader_epoch => INT32
        metadata => COMPACT_NULLABLE_STRING
        error_code => INT16
    error_code => INT16
*/
export const OFFSET_FETCH = createApi<OffsetFetchRequest, OffsetFetchResponse>({
    apiKey: 9,
    apiVersion: 8,
    fallback: OFFSET_FETCH_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.groups, (encoder, group) =>
                encoder
                    .writeCompactString(group.groupId)
                    .writeCompactArray(group.topics, (encoder, topic) =>
                        encoder
                            .writeCompactString(topic.name)
                            .writeCompactArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                                encoder.writeInt32(partitionIndex),
                            )
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeBoolean(data.requireStable)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            groups: decoder.readCompactArray((decoder) => ({
                groupId: decoder.readCompactString()!,
                topics: decoder.readCompactArray((decoder) => ({
                    name: decoder.readCompactString()!,
                    partitions: decoder.readCompactArray((decoder) => ({
                        partitionIndex: decoder.readInt32(),
                        committedOffset: decoder.readInt64(),
                        committedLeaderEpoch: decoder.readInt32(),
                        committedMetadata: decoder.readCompactString(),
                        errorCode: decoder.readInt16(),
                        tags: decoder.readTagBuffer(),
                    })),
                    tags: decoder.readTagBuffer(),
                })),
                errorCode: decoder.readInt16(),
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        result.groups.forEach((group) => {
            if (group.errorCode) throw new KafkaTSApiError(group.errorCode, null, result);
            group.topics.forEach((topic) => {
                topic.partitions.forEach((partition) => {
                    if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
                });
            });
        });
        return result;
    },
});
