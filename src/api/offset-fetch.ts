import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const OFFSET_FETCH = createApi({
    apiKey: 9,
    apiVersion: 8,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (
        encoder,
        data: {
            groups: {
                groupId: string;
                topics: {
                    name: string;
                    partitionIndexes: number[];
                }[];
            }[];
            requireStable: boolean;
        },
    ) =>
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
                groupId: decoder.readCompactString(),
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
