import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const OFFSET_COMMIT = createApi({
    apiKey: 8,
    apiVersion: 8,
    request: (
        encoder,
        data: {
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
        },
    ) =>
        encoder
            .writeUVarInt(0)
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
                            .writeUVarInt(0),
                    )
                    .writeUVarInt(0),
            )
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((decoder) => ({
                name: decoder.readCompactString(),
                partitions: decoder.readCompactArray((decoder) => ({
                    partitionIndex: decoder.readInt32(),
                    errorCode: decoder.readInt16(),
                    _tag: decoder.readTagBuffer(),
                })),
                _tag: decoder.readTagBuffer(),
            })),
            _tag2: decoder.readTagBuffer(),
        };
        result.topics.forEach((topic) => {
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});
