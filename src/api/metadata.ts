import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';

export type Metadata = Awaited<ReturnType<(typeof METADATA)['response']>>;

export const METADATA = createApi({
    apiKey: 3,
    apiVersion: 12,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (
        encoder,
        data: {
            topics?: { id: string | null; name: string }[] | null;
            allowTopicAutoCreation?: boolean;
            includeTopicAuthorizedOperations?: boolean;
        },
    ) =>
        encoder
            .writeCompactArray(data.topics ?? null, (encoder, topic) =>
                encoder.writeUUID(topic.id).writeCompactString(topic.name).writeTagBuffer(),
            )
            .writeBoolean(data.allowTopicAutoCreation ?? false)
            .writeBoolean(data.includeTopicAuthorizedOperations ?? false)
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            brokers: decoder.readCompactArray((broker) => ({
                nodeId: broker.readInt32(),
                host: broker.readCompactString()!,
                port: broker.readInt32(),
                rack: broker.readCompactString(),
                tags: broker.readTagBuffer(),
            })),
            clusterId: decoder.readCompactString(),
            controllerId: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                errorCode: topic.readInt16(),
                name: topic.readCompactString()!,
                topicId: topic.readUUID(),
                isInternal: topic.readBoolean(),
                partitions: topic.readCompactArray((partition) => ({
                    errorCode: partition.readInt16(),
                    partitionIndex: partition.readInt32(),
                    leaderId: partition.readInt32(),
                    leaderEpoch: partition.readInt32(),
                    replicaNodes: partition.readCompactArray((node) => node.readInt32()),
                    isrNodes: partition.readCompactArray((node) => node.readInt32()),
                    offlineReplicas: partition.readCompactArray((node) => node.readInt32()),
                    tags: partition.readTagBuffer(),
                })),
                topicAuthorizedOperations: topic.readInt32(),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        result.topics.forEach((topic) => {
            if (topic.errorCode) throw new KafkaTSApiError(topic.errorCode, null, result);
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});
