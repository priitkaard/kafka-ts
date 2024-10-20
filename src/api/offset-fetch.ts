import { createApi } from "../utils/api";
import { KafkaTSApiError } from "../utils/error";

export const OFFSET_FETCH = createApi({
    apiKey: 9,
    apiVersion: 9,
    request: (
        encoder,
        data: {
            groups: {
                groupId: string;
                memberId: string | null;
                memberEpoch: number;
                topics: {
                    name: string;
                    partitionIndexes: number[];
                }[];
            }[];
            requireStable: boolean;
        },
    ) =>
        encoder
            .writeUVarInt(0)
            .writeCompactArray(data.groups, (encoder, group) =>
                encoder
                    .writeCompactString(group.groupId)
                    .writeCompactString(group.memberId)
                    .writeInt32(group.memberEpoch)
                    .writeCompactArray(group.topics, (encoder, topic) =>
                        encoder
                            .writeCompactString(topic.name)
                            .writeCompactArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                                encoder.writeInt32(partitionIndex),
                            )
                            .writeUVarInt(0),
                    )
                    .writeUVarInt(0),
            )
            .writeBoolean(data.requireStable)
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            groups: decoder.readCompactArray((decoder) => ({
                groupId: decoder.readCompactString(),
                topics: decoder.readCompactArray((decoder) => ({
                    name: decoder.readCompactString()!,
                    partitions: decoder.readCompactArray((decoder) => ({
                        partitionIndex: decoder.readInt32(),
                        committedOffset: decoder.readInt64(),
                        committedLeaderEpoch: decoder.readInt32(),
                        committedMetadata: decoder.readCompactString(),
                        errorCode: decoder.readInt16(),
                        _tag: decoder.readTagBuffer(),
                    })),
                    _tag: decoder.readTagBuffer(),
                })),
                errorCode: decoder.readInt16(),
                _tag: decoder.readTagBuffer(),
            })),
            _tag2: decoder.readTagBuffer(),
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
