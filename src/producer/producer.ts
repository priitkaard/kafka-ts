import { API_ERROR, handleApiError } from '../api';
import { Cluster } from '../cluster';
import { groupByLeaderId } from '../distributors/group-by-leader-id';
import { defaultPartitioner, Partition, Partitioner } from '../distributors/partitioner';
import { Metadata } from '../metadata';
import { Message } from '../types';
import { delay } from '../utils/delay';
import { ConnectionError, KafkaTSApiError } from '../utils/error';
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
    maxRetries?: number;
    retryDelayMs?: number;
    maxRetryDelayMs?: number;
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
            maxBatchSize: options.maxBatchSize ?? 500,
            maxRetries: options.maxRetries ?? 5,
            retryDelayMs: options.retryDelayMs ?? 100,
            maxRetryDelayMs: options.maxRetryDelayMs ?? 3_000,
        };
        this.metadata = new Metadata({ cluster });
        this.state = new ProducerState({ cluster });
        this.partition = this.options.partitioner({ metadata: this.metadata });
    }

    @trace(() => ({ root: true }))
    public async send(messages: Message[]) {
        return this.sendBatch(messages, 0);
    }

    public async close() {
        await this.cluster.disconnect();
    }

    private async sendBatch(messages: Message[], attempt: number): Promise<void> {
        await this.ensureProducerInitialized();

        const topics = [...new Set(messages.map((message) => message.topic))];
        await this.fetchMetadataForTopics(topics);

        const partitionedMessages = messages.map((message) => {
            message.partition = this.partition(message);
            return message as typeof message & { partition: number };
        });
        const messagesByLeaderId = groupByLeaderId(partitionedMessages, this.metadata.getTopicPartitionLeaderIds());

        await Promise.all(
            Object.entries(messagesByLeaderId).map(async ([leaderId, leaderMessages]) => {
                const nodeId = parseInt(leaderId);
                const buffer = (this.bufferByNodeId[nodeId] ??= new ProducerBuffer({
                    nodeId,
                    maxBatchSize: this.options.maxBatchSize,
                    cluster: this.cluster,
                    state: this.state,
                }));
                try {
                    await buffer.enqueue(leaderMessages);
                } catch (error) {
                    if (attempt >= this.options.maxRetries) {
                        log.warn('Retries exhausted', { nodeId, lastError: error });
                        throw error;
                    }
                    await this.handleError(error, nodeId);
                    await delay(this.getRetryDelayMs(attempt));

                    return this.sendBatch(leaderMessages, attempt + 1);
                }
            }),
        );
    }

    private getRetryDelayMs(attempt: number) {
        const { retryDelayMs, maxRetryDelayMs } = this.options;
        return Math.min(maxRetryDelayMs, retryDelayMs * 2 ** attempt);
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

    private reconnect = shared(async () => {
        this.bufferByNodeId = {};
        this.state.reset();
        await this.cluster.disconnect().catch((error) => {
            log.debug('Failed to disconnect cluster', { reason: (error as Error).message });
        });
    });

    private async handleError(error: unknown, nodeId: number): Promise<void> {
        if (error instanceof ConnectionError) {
            log.debug('Connection error while producing. Reconnecting...', {
                nodeId,
                reason: error.message,
            });
            await this.reconnect();
            return;
        }

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
            const fencedErrorCodes: number[] = [
                API_ERROR.UNKNOWN_PRODUCER_ID,
                API_ERROR.INVALID_PRODUCER_EPOCH,
                API_ERROR.PRODUCER_FENCED,
            ];
            if (error instanceof KafkaTSApiError && fencedErrorCodes.includes(error.errorCode)) {
                log.debug('Producer ID is no longer valid. Reinitializing producer ID', { reason: error.message });
                this.state.reset();
                await this.state.initProducerId();
                return;
            }
            throw error;
        });
    }
}
