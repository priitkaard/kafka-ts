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
import { isProducerIdError, ProducerState } from './producer-state';

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
    private closed = false;

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
        await this.cluster.ensureConnected();

        const topics = [...new Set(messages.map((message) => message.topic))];
        await this.fetchMetadataForTopics(topics);

        const partitionedMessages = messages.map((message) => {
            message.partition = this.partition(message);
            return message as typeof message & { partition: number };
        });
        const messagesByLeaderId = groupByLeaderId(partitionedMessages, this.metadata.getTopicPartitionLeaderIds());

        await Promise.all(
            Object.entries(messagesByLeaderId).map(([leaderId, leaderMessages]) => {
                const nodeId = parseInt(leaderId);
                const buffer = (this.bufferByNodeId[nodeId] ??= new ProducerBuffer({
                    maxBatchSize: this.options.maxBatchSize,
                    cluster: this.cluster,
                    metadata: this.metadata,
                    state: this.state,
                    partitionLocks: this.chain,
                    retry: this.retry,
                }));
                return buffer.enqueue(leaderMessages);
            }),
        );
    }

    public async close() {
        this.closed = true;
        await this.cluster.disconnect();
    }

    private retry = async (error: unknown, attempt: number) => {
        if (this.closed) throw error;
        if (attempt >= this.options.maxRetries) {
            log.warn('Retries exhausted', { lastError: error });
            throw error;
        }
        await this.handleError(error);
        await delay(this.getRetryDelayMs(attempt));
        if (this.closed) throw error;
    };

    private getRetryDelayMs(attempt: number) {
        const { retryDelayMs, maxRetryDelayMs } = this.options;
        return Math.min(maxRetryDelayMs, retryDelayMs * 2 ** attempt);
    }

    private fetchMetadataForTopics = shared(async (topics: string[]) => {
        const { allowTopicAutoCreation } = this.options;
        await this.chain.run(
            topics.map((topic) => `metadata:${topic}`),
            () => this.metadata.fetchMetadataIfNecessary({ topics, allowTopicAutoCreation }),
        );
    });

    private refreshMetadata = shared(async () => {
        await this.cluster.ensureConnected();
        const topics = Object.keys(this.metadata.getTopicPartitions());
        await this.metadata.fetchMetadata({ topics, allowTopicAutoCreation: false });
    });

    private async handleError(error: unknown): Promise<void> {
        if (error instanceof ConnectionError) {
            log.debug('Connection error while producing. Refreshing metadata...', { reason: error.message });
            await this.refreshMetadata();
            return;
        }

        await handleApiError(error).catch(async (error) => {
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.NOT_LEADER_OR_FOLLOWER) {
                log.debug('Refreshing metadata', { reason: error.message });
                await this.refreshMetadata();
                return;
            }
            if (isProducerIdError(error)) {
                log.debug('Producer ID is no longer valid. Retrying with a new producer ID', { reason: error.message });
                return;
            }
            throw error;
        });
    }
}
