import { API, API_ERROR } from '../api';
import { Cluster } from '../cluster';
import { distributeMessagesToTopicPartitionLeaders } from '../distributors/messages-to-topic-partition-leaders';
import { defaultPartitioner, Partition, Partitioner } from '../distributors/partitioner';
import { Metadata } from '../metadata';
import { Message } from '../types';
import { delay } from '../utils/delay';
import { KafkaTSApiError } from '../utils/error';
import { createTracer } from '../utils/tracer';

const trace = createTracer('Producer');

export type ProducerOptions = {
    allowTopicAutoCreation?: boolean;
    partitioner?: Partitioner;
};

export class Producer {
    private options: Required<ProducerOptions>;
    private metadata: Metadata;
    private producerId = 0n;
    private producerEpoch = 0;
    private sequences: Record<string, Record<number, number>> = {};
    private partition: Partition;

    constructor(
        private cluster: Cluster,
        options: ProducerOptions,
    ) {
        this.options = {
            ...options,
            allowTopicAutoCreation: options.allowTopicAutoCreation ?? false,
            partitioner: options.partitioner ?? defaultPartitioner,
        };
        this.metadata = new Metadata({ cluster });
        this.partition = this.options.partitioner({ metadata: this.metadata });
    }

    @trace(() => ({ root: true }))
    public async send(messages: Message[], { acks = -1 }: { acks?: -1 | 1 } = {}) {
        await this.ensureConnected();

        const { allowTopicAutoCreation } = this.options;
        const defaultTimestamp = BigInt(Date.now());

        const topics = Array.from(new Set(messages.map((message) => message.topic)));
        await this.metadata.fetchMetadataIfNecessary({ topics, allowTopicAutoCreation });

        const nodeTopicPartitionMessages = distributeMessagesToTopicPartitionLeaders(
            messages.map((message) => ({ ...message, partition: this.partition(message) })),
            this.metadata.getTopicPartitionLeaderIds(),
        );

        try {
            await Promise.all(
                Object.entries(nodeTopicPartitionMessages).map(async ([nodeId, topicPartitionMessages]) => {
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
                                producerId: this.producerId,
                                producerEpoch: 0,
                                baseSequence: this.getSequence(topic, partitionIndex),
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
                    await this.cluster.sendRequestToNode(parseInt(nodeId))(API.PRODUCE, {
                        transactionalId: null,
                        acks,
                        timeoutMs: 5000,
                        topicData,
                    });
                    topicData.forEach(({ name, partitionData }) => {
                        partitionData.forEach(({ index, records }) => {
                            this.updateSequence(name, index, records.length);
                        });
                    });
                }),
            );
        } catch (error) {
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.NOT_LEADER_OR_FOLLOWER) {
                await this.metadata.fetchMetadata({ topics, allowTopicAutoCreation });
            }
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.OUT_OF_ORDER_SEQUENCE_NUMBER) {
                await this.initProducerId();
            }
            throw error;
        }
    }

    public async close() {
        await this.cluster.disconnect();
    }

    private async ensureConnected() {
        await this.cluster.ensureConnected();
        if (!this.producerId) {
            await this.initProducerId();
        }
    }

    private async initProducerId(): Promise<void> {
        try {
            const result = await this.cluster.sendRequest(API.INIT_PRODUCER_ID, {
                transactionalId: null,
                transactionTimeoutMs: 0,
                producerId: this.producerId,
                producerEpoch: this.producerEpoch,
            });
            this.producerId = result.producerId;
            this.producerEpoch = result.producerEpoch;
            this.sequences = {};
        } catch (error) {
            if ((error as KafkaTSApiError).errorCode === API_ERROR.COORDINATOR_LOAD_IN_PROGRESS) {
                await delay(100);
                return this.initProducerId();
            }
            throw error;
        }
    }

    private getSequence(topic: string, partition: number) {
        return this.sequences[topic]?.[partition] ?? 0;
    }

    private updateSequence(topic: string, partition: number, messagesCount: number) {
        this.sequences[topic] ??= {};
        this.sequences[topic][partition] ??= 0;
        this.sequences[topic][partition] += messagesCount;
    }
}
