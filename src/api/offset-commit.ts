import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

type OffsetCommitRequest = {
    groupId: string;
    generationIdOrMemberEpoch: number;
    memberId: string;
    groupInstanceId: string | null;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            committedOffset: bigint;
            committedLeaderEpoch: number;
            committedMetadata: string | null;
        }[];
    }[];
};

type OffsetCommitResponse = {
    throttleTimeMs: number;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

/*
OffsetCommit Request (Version: 2) => group_id generation_id_or_member_epoch member_id retention_time_ms [topics] 
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  retention_time_ms => INT64
  topics => name [partitions] 
    name => STRING
    partitions => partition_index committed_offset committed_metadata 
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING

OffsetCommit Response (Version: 2) => [topics] 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code 
      partition_index => INT32
      error_code => INT16
*/
const OFFSET_COMMIT_V2 = createApi<OffsetCommitRequest, OffsetCommitResponse>({
    apiKey: 8,
    apiVersion: 2,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.generationIdOrMemberEpoch)
            .writeString(data.memberId)
            .writeInt64(-1n) // retention_time_ms is deprecated and should be set to -1
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt64(partition.committedOffset)
                            .writeString(partition.committedMetadata),
                    ),
            ),
    response: (decoder) => {
        const result = {
            throttleTimeMs: 0,
            topics: decoder.readArray((decoder) => ({
                name: decoder.readString()!,
                partitions: decoder.readArray((decoder) => ({
                    partitionIndex: decoder.readInt32(),
                    errorCode: decoder.readInt16(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        };
        result.topics.forEach((topic) => {
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});

/*
OffsetCommit Request (Version: 8) => group_id generation_id_or_member_epoch member_id group_instance_id [topics] _tagged_fields 
  group_id => COMPACT_STRING
  generation_id_or_member_epoch => INT32
  member_id => COMPACT_STRING
  group_instance_id => COMPACT_NULLABLE_STRING
  topics => name [partitions] _tagged_fields 
    name => COMPACT_STRING
    partitions => partition_index committed_offset committed_leader_epoch committed_metadata _tagged_fields 
      partition_index => INT32
      committed_offset => INT64
      committed_leader_epoch => INT32
      committed_metadata => COMPACT_NULLABLE_STRING

OffsetCommit Response (Version: 8) => throttle_time_ms [topics] _tagged_fields 
  throttle_time_ms => INT32
  topics => name [partitions] _tagged_fields 
    name => COMPACT_STRING
    partitions => partition_index error_code _tagged_fields 
      partition_index => INT32
      error_code => INT16
*/
export const OFFSET_COMMIT = createApi<OffsetCommitRequest, OffsetCommitResponse>({
    apiKey: 8,
    apiVersion: 8,
    fallback: OFFSET_COMMIT_V2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeInt32(data.generationIdOrMemberEpoch)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt64(partition.committedOffset)
                            .writeInt32(partition.committedLeaderEpoch)
                            .writeCompactString(partition.committedMetadata)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((decoder) => ({
                name: decoder.readCompactString()!,
                partitions: decoder.readCompactArray((decoder) => ({
                    partitionIndex: decoder.readInt32(),
                    errorCode: decoder.readInt16(),
                    tags: decoder.readTagBuffer(),
                })),
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        result.topics.forEach((topic) => {
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});
