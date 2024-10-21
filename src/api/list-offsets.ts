import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';
import { IsolationLevel } from './fetch';

export const LIST_OFFSETS = createApi({
    apiKey: 2,
    apiVersion: 8,
    request: (
        encoder,
        data: {
            replicaId: number;
            isolationLevel: IsolationLevel;
            topics: {
                name: string;
                partitions: {
                    partitionIndex: number;
                    currentLeaderEpoch: number;
                    timestamp: bigint;
                }[];
            }[];
        },
    ) =>
        encoder
            .writeUVarInt(0)
            .writeInt32(data.replicaId)
            .writeInt8(data.isolationLevel)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt32(partition.currentLeaderEpoch)
                            .writeInt64(partition.timestamp)
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
                name: decoder.readCompactString()!,
                partitions: decoder.readCompactArray((decoder) => ({
                    partitionIndex: decoder.readInt32(),
                    errorCode: decoder.readInt16(),
                    timestamp: decoder.readInt64(),
                    offset: decoder.readInt64(),
                    leaderEpoch: decoder.readInt32(),
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
