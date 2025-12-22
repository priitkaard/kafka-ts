import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export const CREATE_TOPICS = createApi({
    apiKey: 19,
    apiVersion: 7,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (
        encoder,
        data: {
            topics: {
                name: string;
                numPartitions?: number;
                replicationFactor?: number;
                assignments?: {
                    partitionIndex: number;
                    brokerIds: number[];
                }[];
                configs?: {
                    name: string;
                    value: string | null;
                }[];
            }[];
            timeoutMs?: number;
            validateOnly?: boolean;
        },
    ) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeInt32(topic.numPartitions ?? -1)
                    .writeInt16(topic.replicationFactor ?? -1)
                    .writeCompactArray(topic.assignments ?? [], (encoder, assignment) =>
                        encoder
                            .writeInt32(assignment.partitionIndex)
                            .writeCompactArray(assignment.brokerIds, (encoder, brokerId) =>
                                encoder.writeInt32(brokerId),
                            )
                            .writeTagBuffer(),
                    )
                    .writeCompactArray(topic.configs ?? [], (encoder, config) =>
                        encoder.writeCompactString(config.name).writeCompactString(config.value).writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeInt32(data.timeoutMs ?? 10_000)
            .writeBoolean(data.validateOnly ?? false)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
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
                    tags: config.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        result.topics.forEach((topic) => {
            if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, topic.errorMessage, result);
        });
        return result;
    },
});
