import { API, API_ERROR } from './api';
import { Cluster } from './cluster';
import { delay } from './utils/delay';
import { KafkaTSApiError } from './utils/error';

type MetadataOptions = {
    cluster: Cluster;
};

export class Metadata {
    private topicPartitions: Record<string, number[]> = {};
    private topicNameById: Record<string, string> = {};
    private topicIdByName: Record<string, string> = {};
    private leaderIdByTopicPartition: Record<string, Record<number, number>> = {};
    private isrNodesByTopicPartition: Record<string, Record<number, number[]>> = {};

    constructor(private options: MetadataOptions) {}

    public getTopicPartitionLeaderIds() {
        return this.leaderIdByTopicPartition;
    }

    public getTopicPartitionReplicaIds() {
        return this.isrNodesByTopicPartition;
    }

    public getTopicPartitions() {
        return this.topicPartitions;
    }

    public getTopicIdByName(name: string) {
        return this.topicIdByName[name];
    }

    public getTopicNameById(id: string) {
        return this.topicNameById[id];
    }

    public async fetchMetadataIfNecessary({
        topics,
        allowTopicAutoCreation,
    }: {
        topics: string[];
        allowTopicAutoCreation: boolean;
    }) {
        const missingTopics = topics.filter((topic) => !this.topicPartitions[topic]);
        if (!missingTopics.length) {
            return;
        }

        try {
            return await this.fetchMetadata({ topics: missingTopics, allowTopicAutoCreation });
        } catch (error) {
            if (
                error instanceof KafkaTSApiError &&
                error.errorCode === API_ERROR.UNKNOWN_TOPIC_OR_PARTITION &&
                allowTopicAutoCreation
            ) {
                // TODO: investigate if we can avoid the delay
                await delay(1000);
                return await this.fetchMetadata({ topics: missingTopics, allowTopicAutoCreation });
            }
            throw error;
        }
    }

    private async fetchMetadata({
        topics,
        allowTopicAutoCreation,
    }: {
        topics: string[] | null;
        allowTopicAutoCreation: boolean;
    }) {
        const { cluster } = this.options;

        const response = await cluster.sendRequest(API.METADATA, {
            allowTopicAutoCreation,
            includeTopicAuthorizedOperations: false,
            topics: topics?.map((name) => ({ id: null, name })) ?? null,
        });

        this.topicPartitions = {
            ...this.topicPartitions,
            ...Object.fromEntries(
                response.topics.map((topic) => [
                    topic.name,
                    topic.partitions.map((partition) => partition.partitionIndex),
                ]),
            ),
        };
        this.topicNameById = {
            ...this.topicNameById,
            ...Object.fromEntries(response.topics.map((topic) => [topic.topicId, topic.name])),
        };
        this.topicIdByName = {
            ...this.topicIdByName,
            ...Object.fromEntries(response.topics.map((topic) => [topic.name, topic.topicId])),
        };
        this.leaderIdByTopicPartition = {
            ...this.leaderIdByTopicPartition,
            ...Object.fromEntries(
                response.topics.map((topic) => [
                    topic.name,
                    Object.fromEntries(
                        topic.partitions.map((partition) => [partition.partitionIndex, partition.leaderId]),
                    ),
                ]),
            ),
        };
        this.isrNodesByTopicPartition = {
            ...this.isrNodesByTopicPartition,
            ...Object.fromEntries(
                response.topics.map((topic) => [
                    topic.name,
                    Object.fromEntries(
                        topic.partitions.map((partition) => [partition.partitionIndex, partition.isrNodes]),
                    ),
                ]),
            ),
        };
    }
}
