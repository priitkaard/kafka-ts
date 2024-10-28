import { API } from '../api';
import { IsolationLevel } from '../api/fetch';
import { Assignment } from '../api/sync-group';
import { Cluster } from '../cluster';
import { distributeMessagesToTopicPartitionLeaders } from '../distributors/messages-to-topic-partition-leaders';
import { createTracer } from '../utils/tracer';
import { ConsumerMetadata } from './consumer-metadata';

const trace = createTracer('OffsetManager');

type OffsetManagerOptions = {
    cluster: Cluster;
    metadata: ConsumerMetadata;
    isolationLevel: IsolationLevel;
};

export class OffsetManager {
    private currentOffsets: Record<string, Record<number, bigint>> = {};
    public pendingOffsets: Record<string, Record<number, bigint>> = {};

    constructor(private options: OffsetManagerOptions) {}

    public getCurrentOffset(topic: string, partition: number) {
        return this.currentOffsets[topic]?.[partition] ?? 0n;
    }

    public resolve(topic: string, partition: number, offset: bigint) {
        this.pendingOffsets[topic] ??= {};
        this.pendingOffsets[topic][partition] = offset;
    }

    public flush(topicPartitions: Record<string, Set<number>>) {
        Object.entries(topicPartitions).forEach(([topic, partitions]) => {
            this.currentOffsets[topic] ??= {};
            partitions.forEach((partition) => {
                if (this.pendingOffsets[topic]?.[partition]) {
                    this.currentOffsets[topic][partition] = this.pendingOffsets[topic][partition];
                    delete this.pendingOffsets[topic][partition];
                }
            });
        });
    }

    public async fetchOffsets(options: { fromBeginning: boolean }) {
        const { metadata } = this.options;

        const topicPartitions = Object.entries(metadata.getAssignment()).flatMap(([topic, partitions]) =>
            partitions.map((partition) => ({ topic, partition })),
        );
        const nodeTopicPartitions = distributeMessagesToTopicPartitionLeaders(
            topicPartitions,
            metadata.getTopicPartitionLeaderIds(),
        );

        await Promise.all(
            Object.entries(nodeTopicPartitions).map(([nodeId, topicPartitions]) =>
                this.listOffsets({
                    ...options,
                    nodeId: parseInt(nodeId),
                    nodeAssignment: Object.fromEntries(
                        Object.entries(topicPartitions).map(
                            ([topicName, partitions]) =>
                                [topicName, Object.keys(partitions).map(Number)] as [string, number[]],
                        ),
                    ),
                }),
            ),
        );
    }

    private async listOffsets({
        nodeId,
        nodeAssignment,
        fromBeginning,
    }: {
        nodeId: number;
        nodeAssignment: Assignment;
        fromBeginning: boolean;
    }) {
        const { cluster, isolationLevel } = this.options;

        const offsets = await cluster.sendRequestToNode(nodeId)(API.LIST_OFFSETS, {
            replicaId: -1,
            isolationLevel,
            topics: Object.entries(nodeAssignment)
                .flatMap(([topic, partitions]) => partitions.map((partition) => ({ topic, partition })))
                .map(({ topic, partition }) => ({
                    name: topic,
                    partitions: [{ partitionIndex: partition, currentLeaderEpoch: -1, timestamp: -1n }],
                })),
        });

        const topicPartitions: Record<string, Set<number>> = {};
        offsets.topics.forEach(({ name, partitions }) => {
            topicPartitions[name] ??= new Set();
            partitions.forEach(({ partitionIndex, offset }) => {
                topicPartitions[name].add(partitionIndex);
                this.resolve(name, partitionIndex, fromBeginning ? 0n : offset);
            });
        });

        this.flush(topicPartitions);
    }
}
