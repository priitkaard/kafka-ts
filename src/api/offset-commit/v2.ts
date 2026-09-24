import { createApi } from '../../utils/api';
import { OffsetCommitRequest, OffsetCommitResponse, throwIfError } from './common';

/*
OffsetCommit Request (Version: 2) => { group_id generation_id_or_member_epoch member_id retention_time_ms [topics] }
  group_id => STRING
  generation_id_or_member_epoch => INT32
  member_id => STRING
  retention_time_ms => INT64
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index committed_offset committed_metadata }
      partition_index => INT32
      committed_offset => INT64
      committed_metadata => NULLABLE_STRING

OffsetCommit Response (Version: 2) => { [topics] }
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const OFFSET_COMMIT_V2 = createApi<OffsetCommitRequest, OffsetCommitResponse>({
    apiKey: 8,
    apiVersion: 2,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.groupId)
            .writeInt32(data.generationIdOrMemberEpoch)
            .writeString(data.memberId)
            .writeInt64(-1n)
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
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: 0,
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
