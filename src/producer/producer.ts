import { API_ERROR, handleApiError } from '../api';
import { Cluster } from '../cluster';
import { groupByLeaderId } from '../distributors/group-by-leader-id';
import { defaultPartitioner, Partition, Partitioner } from '../distributors/partitioner';
import { Metadata } from '../metadata';
import { Message } from '../types';
import { KafkaTSApiError } from '../utils/error';
import { log } from '../utils/logger';
import { PromiseChain } from '../utils/promise-chain';
import { shared } from '../utils/shared';
import { createTracer } from '../utils/tracer';
import { ProducerBuffer } from './producer-buffer';
import { ProducerState } from './producer-state';

const trace = createTracer('Producer');

export type ProducerOptions = {
    allowTopicAutoCreation?: boolean;
    partitioner?: Partitioner;
    maxBatchSize?: number;
};

export class Producer {
    private options: Required<ProducerOptions>;
    private metadata: Metadata;
    private state: ProducerState;
    private partition: Partition;
    private chain = new PromiseChain();
    private bufferByNodeId: Record<number, ProducerBuffer> = {};

    constructor(
        private cluster: Cluster,
        options: ProducerOptions,
    ) {
        this.options = {
            ...options,
            allowTopicAutoCreation: options.allowTopicAutoCreation ?? false,
            partitioner: options.partitioner ?? defaultPartitioner,
            maxBatchSize: options.maxBatchSize ?? 50,
        };
        this.metadata = new Metadata({ cluster });
        this.state = new ProducerState({ cluster });
        this.partition = this.options.partitioner({ metadata: this.metadata });
    }

    @trace(() => ({ root: true }))
    public async send(messages: Message[]) {
        await this.ensureProducerInitialized();

        const topics = [...new Set(messages.map((message) => message.topic))];
        await this.fetchMetadataForTopics(topics);

        const partitionedMessages = messages.map((message) => {
            message.partition = this.partition(message);
            return message as typeof message & { partition: number };
        });
        const messagesByLeaderId = groupByLeaderId(partitionedMessages, this.metadata.getTopicPartitionLeaderIds());

        await Promise.all(
            Object.entries(messagesByLeaderId).map(async ([leaderId, messages]) => {
                const nodeId = parseInt(leaderId);
                const buffer = (this.bufferByNodeId[nodeId] ??= new ProducerBuffer({
                    nodeId,
                    maxBatchSize: this.options.maxBatchSize,
                    cluster: this.cluster,
                    state: this.state,
                }));
                try {
                    await buffer.enqueue(messages);
                } catch (error) {
                    await this.handleError(error);
                    return this.send(messages);
                }
            }),
        );
    }

    public async close() {
        await this.cluster.disconnect();
    }

    private ensureProducerInitialized = shared(async () => {
        await this.cluster.ensureConnected();
        if (!this.state.producerId) {
            await this.state.initProducerId();
        }
    });

    private fetchMetadataForTopics = shared(async (topics: string[]) => {
        const { allowTopicAutoCreation } = this.options;
        await this.chain.run(
            topics.map((topic) => `metadata:${topic}`),
            () => this.metadata.fetchMetadataIfNecessary({ topics, allowTopicAutoCreation }),
        );
    });

    private async handleError(error: unknown): Promise<void> {
        await handleApiError(error).catch(async (error) => {
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.NOT_LEADER_OR_FOLLOWER) {
                log.debug('Refreshing metadata', { reason: error.message });
                const topics = Object.keys(this.metadata.getTopicPartitions());
                await this.metadata.fetchMetadata({ topics, allowTopicAutoCreation: false });
                return;
            }
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.OUT_OF_ORDER_SEQUENCE_NUMBER) {
                log.debug('Out of order sequence number. Reinitializing producer ID');
                await this.state.initProducerId();
                return;
            }
            throw error;
        });
    }
}
