import { API } from '../api';
import { Cluster } from '../cluster';
import { Message } from '../types';
import { ProducerState } from './producer-state';

type BufferEntry = {
    messages: Message[];
    resolve: () => void;
    reject: (error: unknown) => void;
};

type ProducerBufferOptions = {
    nodeId: number;
    maxBatchSize: number;
    cluster: Cluster;
    state: ProducerState;
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

        const { cluster, state, nodeId, maxBatchSize } = this.options;

        while (true) {
            const batch: Message[] = [];
            const resolvers: (() => void)[] = [];
            const rejecters: ((error: unknown) => void)[] = [];

            while (this.head < this.buffer.length) {
                const entry = this.buffer[this.head++];

                batch.push(...entry.messages);
                resolvers.push(entry.resolve);
                rejecters.push(entry.reject);

                const nextLength = this.buffer[this.head]?.messages.length ?? 0;
                if (batch.length + nextLength > maxBatchSize) {
                    break;
                }
            }
            if (!batch.length) break;

            this.compactBuffer();

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
                        producerEpoch: 0,
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
            try {
                await cluster.sendRequestToNode(nodeId)(API.PRODUCE, {
                    transactionalId: null,
                    acks: -1,
                    timeoutMs: 30000,
                    topicData,
                });
                topicData.forEach(({ name, partitionData }) => {
                    partitionData.forEach(({ index, records }) => {
                        state.updateSequence(name, index, records.length);
                    });
                });

                resolvers.forEach((resolve) => resolve());
            } catch (error) {
                rejecters.forEach((reject) => reject(error));
            }
        }

        this.isFlushing = false;
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
