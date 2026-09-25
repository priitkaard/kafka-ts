import { API } from '../api';
import { IsolationLevel } from '../api/fetch';
import { Assignment } from '../api/sync-group';
import { Cluster } from '../cluster';
import { groupByLeaderId } from '../distributors/group-by-leader-id';
import { groupPartitionsByTopic } from '../distributors/group-partitions-by-topic';
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
    private pendingOffsets: Record<string, Record<number, bigint>> = {};

    constructor(private options: OffsetManagerOptions) {}

    public getCurrentOffset(topic: string, partition: number) {
        return this.currentOffsets[topic]?.[partition] ?? 0n;
    }

    public getPendingOffset(topic: string, partition: number) {
        return this.pendingOffsets[topic]?.[partition] ?? 0n;
    }

    public resolve(topic: string, partition: number, offset: bigint) {
        this.pendingOffsets[topic] ??= {};
        this.pendingOffsets[topic][partition] = offset;
    }

    public getPosition(topic: string, partition: number) {
        return this.pendingOffsets[topic]?.[partition] ?? this.getCurrentOffset(topic, partition);
    }

    public getPendingOffsets() {
        return Object.entries(this.options.metadata.getAssignment()).flatMap(([topic, partitions]) =>
            partitions
                .filter((partition) => this.pendingOffsets[topic]?.[partition] !== undefined)
                .map((partition) => ({ topic, partition, offset: this.pendingOffsets[topic][partition] })),
        );
    }

    public markCommitted(offsets: { topic: string; partition: number; offset: bigint }[]) {
        offsets.forEach(({ topic, partition, offset }) => {
            this.currentOffsets[topic] ??= {};
            this.currentOffsets[topic][partition] = offset;
            if (this.pendingOffsets[topic]?.[partition] === offset) {
                delete this.pendingOffsets[topic][partition];
            }
        });
    }

    public getPartitionsWithoutOffset(): Assignment {
        return Object.fromEntries(
            Object.entries(this.options.metadata.getAssignment()).map(([topic, partitions]) => [
                topic,
                partitions.filter((partition) => this.currentOffsets[topic]?.[partition] === undefined),
            ]),
        );
    }

    public isResolved(message: { topic: string; partition: number; offset: bigint }) {
        return (
            this.getCurrentOffset(message.topic, message.partition) > message.offset ||
            this.getPendingOffset(message.topic, message.partition) > message.offset
        );
    }

    public flush(topicPartitions: Record<string, Set<number>>) {
        Object.entries(topicPartitions).forEach(([topic, partitions]) => {
            this.currentOffsets[topic] ??= {};
            partitions.forEach((partition) => {
                if (this.pendingOffsets[topic]?.[partition] !== undefined) {
                    this.currentOffsets[topic][partition] = this.pendingOffsets[topic][partition];
                    delete this.pendingOffsets[topic][partition];
                }
            });
        });
    }

    public async fetchOffsets({
        fromTimestamp,
        assignment = this.options.metadata.getAssignment(),
    }: {
        fromTimestamp: bigint;
        assignment?: Assignment;
    }) {
        const { metadata } = this.options;

        const topicPartitions = Object.entries(assignment).flatMap(([topic, partitions]) =>
            partitions.map((partition) => ({ topic, partition })),
        );
        const topicPartitionsByLeaderId = groupByLeaderId(topicPartitions, metadata.getTopicPartitionLeaderIds());

        await Promise.all(
            Object.entries(topicPartitionsByLeaderId).map(([leaderId, topicPartitions]) =>
                this.listOffsets({
                    fromTimestamp,
                    nodeId: parseInt(leaderId),
                    nodeAssignment: groupPartitionsByTopic(topicPartitions),
                }),
            ),
        );
    }

    private async listOffsets({
        nodeId,
        nodeAssignment,
        fromTimestamp,
    }: {
        nodeId: number;
        nodeAssignment: Assignment;
        fromTimestamp: bigint;
    }) {
        const { cluster, isolationLevel } = this.options;

        const offsets = await cluster.sendRequestToNode(nodeId)(API.LIST_OFFSETS, {
            replicaId: -1,
            isolationLevel,
            topics: Object.entries(nodeAssignment)
                .flatMap(([topic, partitions]) => partitions.map((partition) => ({ topic, partition })))
                .map(({ topic, partition }) => ({
                    name: topic,
                    partitions: [
                        {
                            partitionIndex: partition,
                            currentLeaderEpoch: -1,
                            timestamp: fromTimestamp,
                        },
                    ],
                })),
        });

        const topicPartitions: Record<string, Set<number>> = {};
        offsets.topics.forEach(({ name, partitions }) => {
            topicPartitions[name] ??= new Set();
            partitions.forEach(({ partitionIndex, offset }) => {
                topicPartitions[name].add(partitionIndex);
                this.resolve(name, partitionIndex, offset);
            });
        });

        this.flush(topicPartitions);
    }
}
