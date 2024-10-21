import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const CREATE_TOPICS = createApi({
    apiKey: 19,
    apiVersion: 7,
    request: (
        encoder,
        data: {
            topics: {
                name: string;
                numPartitions: number;
                replicationFactor: number;
                assignments: {
                    partitionIndex: number;
                    brokerIds: number[];
                }[];
                configs: {
                    name: string;
                    value: string | null;
                }[];
            }[];
            timeoutMs: number;
            validateOnly: boolean;
        },
    ) =>
        encoder
            .writeUVarInt(0)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeInt32(topic.numPartitions)
                    .writeInt16(topic.replicationFactor)
                    .writeCompactArray(topic.assignments, (encoder, assignment) =>
                        encoder
                            .writeInt32(assignment.partitionIndex)
                            .writeCompactArray(assignment.brokerIds, (encoder, brokerId) =>
                                encoder.writeInt32(brokerId),
                            )
                            .writeUVarInt(0),
                    )
                    .writeCompactArray(topic.configs, (encoder, config) =>
                        encoder.writeCompactString(config.name).writeCompactString(config.value).writeUVarInt(0),
                    )
                    .writeUVarInt(0),
            )
            .writeInt32(data.timeoutMs)
            .writeBoolean(data.validateOnly)
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                name: topic.readCompactString(),
                topicId: topic.readUUID(),
                errorCode: topic.readInt16(),
                errorMessage: topic.readCompactString(),
                numPartitions: topic.readInt32(),
                replicationFactor: topic.readInt16(),
                configs: topic.readCompactArray((config) => ({
                    name: config.readCompactString(),
                    value: config.readCompactString(),
                    readOnly: config.readBoolean(),
                    configSource: config.readInt8(),
                    isSensitive: config.readBoolean(),
                    _tag: config.readTagBuffer(),
                })),
                _tag: topic.readTagBuffer(),
            })),
            _tag2: decoder.readTagBuffer(),
        };
        result.topics.forEach((topic) => {
            if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, topic.errorMessage, result);
        });
        return result;
    },
});
