import { API } from '../api';
import { Cluster } from '../cluster';
import { groupByLeaderId } from '../distributors/group-by-leader-id';
import { groupPartitionsByTopic } from '../distributors/group-partitions-by-topic';
import { Metadata } from '../metadata';
import { Message } from '../types';
import { PromiseChain } from '../utils/promise-chain';
import { isProducerIdError, ProducerState } from './producer-state';

type BufferEntry = {
    messages: Message[];
    resolve: () => void;
    reject: (error: unknown) => void;
};

type TopicData = Parameters<typeof API.PRODUCE.request>[1]['topicData'];

type ProducerBufferOptions = {
    maxBatchSize: number;
    cluster: Cluster;
    metadata: Metadata;
    state: ProducerState;
    partitionLocks: PromiseChain;
    retry: (error: unknown, attempt: number) => Promise<void>;
};

export class ProducerBuffer {
    private buffer: BufferEntry[] = [];
    private head = 0;
    private isFlushing = false;

    constructor(private options: ProducerBufferOptions) {}

    public enqueue(messages: Message[]): Promise<void> {
        return new Promise((resolve, reject) => {
            this.buffer.push({ messages, resolve, reject });
            this.flush();
        });
    }

    private async flush() {
        if (this.isFlushing) return;
        this.isFlushing = true;

        const { maxBatchSize } = this.options;

        while (true) {
            const batch: Message[] = [];
            const resolvers: (() => void)[] = [];
            const rejecters: ((error: unknown) => void)[] = [];

            while (this.head < this.buffer.length) {
                const entry = this.buffer[this.head++];

                for (const message of entry.messages) batch.push(message);
                resolvers.push(entry.resolve);
                rejecters.push(entry.reject);

                const nextLength = this.buffer[this.head]?.messages.length ?? 0;
                if (batch.length + nextLength > maxBatchSize) {
                    break;
                }
            }
            if (!batch.length) break;

            this.compactBuffer();

            try {
                await this.produce(batch);
                resolvers.forEach((resolve) => resolve());
            } catch (error) {
                rejecters.forEach((reject) => reject(error));
            }
        }

        this.isFlushing = false;
    }

    private async produce(batch: Message[]) {
        const partitions = batch.map((message) => `produce:${message.topic}:${message.partition}`);
        await this.options.partitionLocks.run(partitions, () => this.produceLocked(batch));
    }

    private async produceLocked(batch: Message[]) {
        const { state, retry } = this.options;

        let request: { generation: number; topicData: TopicData } | undefined;
        for (let attempt = 0; ; attempt++) {
            try {
                request ??= await this.createRequest(batch);
                await this.sendToLeaders(request.topicData);
                break;
            } catch (error) {
                try {
                    await retry(error, attempt);
                } catch (retryError) {
                    if (request) state.reset(request.generation);
                    throw retryError;
                }
                if (request && isProducerIdError(error)) {
                    state.reset(request.generation);
                    request = undefined;
                }
            }
        }
        const { generation, topicData } = request;
        if (state.generation !== generation) return;

        topicData.forEach(({ name, partitionData }) => {
            partitionData.forEach(({ index, records }) => {
                state.updateSequence(name, index, records.length);
            });
        });
    }

    private async createRequest(batch: Message[]) {
        const { state } = this.options;

        if (!state.isInitialized) await state.initProducerId();
        const generation = state.generation;

        const topicPartitionMessages: { [topic: string]: { [partition: number]: Message[] } } = {};
        batch.forEach((message) => {
            topicPartitionMessages[message.topic] ??= {};
            topicPartitionMessages[message.topic][message.partition!] ??= [];
            topicPartitionMessages[message.topic][message.partition!].push(message);
        });

        const defaultTimestamp = BigInt(Date.now());

        const topicData = Object.entries(topicPartitionMessages).map(([topic, partitionMessages]) => ({
            name: topic,
            partitionData: Object.entries(partitionMessages).map(([partition, messages]) => {
                const partitionIndex = parseInt(partition);
                let baseTimestamp: bigint | undefined;
                let maxTimestamp: bigint | undefined;

                messages.forEach(({ timestamp = defaultTimestamp }) => {
                    if (!baseTimestamp || timestamp < baseTimestamp) {
                        baseTimestamp = timestamp;
                    }
                    if (!maxTimestamp || timestamp > maxTimestamp) {
                        maxTimestamp = timestamp;
                    }
                });

                return {
                    index: partitionIndex,
                    baseOffset: 0n,
                    partitionLeaderEpoch: -1,
                    attributes: 0,
                    lastOffsetDelta: messages.length - 1,
                    baseTimestamp: baseTimestamp ?? 0n,
                    maxTimestamp: maxTimestamp ?? 0n,
                    producerId: state.producerId,
                    producerEpoch: state.producerEpoch,
                    baseSequence: state.getSequence(topic, partitionIndex),
                    records: messages.map((message, index) => ({
                        attributes: 0,
                        timestampDelta: (message.timestamp ?? defaultTimestamp) - (baseTimestamp ?? 0n),
                        offsetDelta: index,
                        key: message.key ?? null,
                        value: message.value,
                        headers: Object.entries(message.headers ?? {}).map(([key, value]) => ({
                            key,
                            value,
                        })),
                    })),
                };
            }),
        }));
        return { generation, topicData };
    }

    private async sendToLeaders(topicData: TopicData) {
        const { cluster, metadata } = this.options;

        const partitions = topicData.flatMap(({ name, partitionData }) =>
            partitionData.map(({ index }) => ({ topic: name, partition: index })),
        );
        const partitionsByLeaderId = groupByLeaderId(partitions, metadata.getTopicPartitionLeaderIds());

        await Promise.all(
            Object.entries(partitionsByLeaderId).map(([leaderId, leaderPartitions]) => {
                const partitionsByTopic = groupPartitionsByTopic(leaderPartitions);
                return cluster.sendRequestToNode(parseInt(leaderId))(API.PRODUCE, {
                    transactionalId: null,
                    acks: -1,
                    timeoutMs: 30000,
                    topicData: topicData
                        .filter(({ name }) => name in partitionsByTopic)
                        .map(({ name, partitionData }) => ({
                            name,
                            partitionData: partitionData.filter(({ index }) => partitionsByTopic[name].includes(index)),
                        })),
                });
            }),
        );
    }

    private compactBuffer() {
        if (this.head >= this.buffer.length) {
            this.buffer = [];
            this.head = 0;
        } else if (this.head > 1000 && this.head > this.buffer.length / 2) {
            this.buffer = this.buffer.slice(this.head);
            this.head = 0;
        }
    }
}
