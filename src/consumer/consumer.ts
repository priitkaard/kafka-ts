import EventEmitter from 'events';
import { API, API_ERROR, handleApiError } from '../api';
import { FetchResponse, IsolationLevel } from '../api/fetch';
import { Assignment } from '../api/sync-group';
import { Cluster } from '../cluster';
import { groupByLeaderId } from '../distributors/group-by-leader-id';
import { groupPartitionsByTopic } from '../distributors/group-partitions-by-topic';
import { Message } from '../types';
import { delay } from '../utils/delay';
import { ConnectionError, getErrorMessage, KafkaTSApiError } from '../utils/error';
import { log } from '../utils/logger';
import { defaultRetrier, Retrier } from '../utils/retrier';
import { withRetry } from '../utils/retry';
import { createTracer } from '../utils/tracer';
import { ConsumerGroup } from './consumer-group';
import { ConsumerMetadata } from './consumer-metadata';
import { FetchManager } from './fetch-manager';
import { OffsetManager } from './offset-manager';

const trace = createTracer('Consumer');

export type ConsumerOptions = {
    topics: string[];
    groupId?: string | null;
    groupInstanceId?: string | null;
    rackId?: string;
    isolationLevel?: IsolationLevel;
    sessionTimeoutMs?: number;
    rebalanceTimeoutMs?: number;
    maxWaitMs?: number;
    minBytes?: number;
    maxBytes?: number;
    partitionMaxBytes?: number;
    allowTopicAutoCreation?: boolean;
    fromBeginning?: boolean;
    fromTimestamp?: bigint;
    retrier?: Retrier;
    onBatch: (
        messages: Required<Message>[],
        context: {
            resolveOffset: (message: Pick<Required<Message>, 'topic' | 'partition' | 'offset'>) => void;
            abortSignal: AbortSignal;
        },
    ) => unknown;
};

export class Consumer extends EventEmitter<{ offsetCommit: []; heartbeat: []; rebalanceInProgress: [] }> {
    private options: Required<ConsumerOptions>;
    private metadata: ConsumerMetadata;
    private consumerGroup: ConsumerGroup | undefined;
    private offsetManager: OffsetManager;
    private fetchManager?: FetchManager;
    private stopRequested = false;
    private stopHooks: (() => void)[] = [];
    private running = false;
    private closed = false;

    constructor(
        private cluster: Cluster,
        options: ConsumerOptions,
    ) {
        super();

        this.options = {
            ...options,
            groupId: options.groupId ?? null,
            groupInstanceId: options.groupInstanceId ?? null,
            rackId: options.rackId ?? '',
            sessionTimeoutMs: options.sessionTimeoutMs ?? 30_000,
            rebalanceTimeoutMs: options.rebalanceTimeoutMs ?? 60_000,
            maxWaitMs: options.maxWaitMs ?? 5000,
            minBytes: options.minBytes ?? 1,
            maxBytes: options.maxBytes ?? 1_048_576,
            partitionMaxBytes: options.partitionMaxBytes ?? 1_048_576,
            isolationLevel: options.isolationLevel ?? IsolationLevel.READ_COMMITTED,
            allowTopicAutoCreation: options.allowTopicAutoCreation ?? false,
            fromBeginning: options.fromBeginning ?? false,
            fromTimestamp: options.fromTimestamp ?? (options.fromBeginning ? -2n : -1n),
            retrier: options.retrier ?? defaultRetrier,
        };

        this.metadata = new ConsumerMetadata({ cluster: this.cluster });
        this.offsetManager = new OffsetManager({
            cluster: this.cluster,
            metadata: this.metadata,
            isolationLevel: this.options.isolationLevel,
        });
        this.consumerGroup = this.options.groupId
            ? new ConsumerGroup({
                  cluster: this.cluster,
                  topics: this.options.topics,
                  groupId: this.options.groupId,
                  groupInstanceId: this.options.groupInstanceId,
                  sessionTimeoutMs: this.options.sessionTimeoutMs,
                  rebalanceTimeoutMs: this.options.rebalanceTimeoutMs,
                  metadata: this.metadata,
                  offsetManager: this.offsetManager,
                  consumer: this,
              })
            : undefined;

        this.setMaxListeners(Infinity);
    }

    @trace()
    public async start(): Promise<void> {
        this.closed = false;

        return this.resume();
    }

    private async resume(): Promise<void> {
        this.stopRequested = false;
        this.running = true;

        while (true) {
            try {
                await this.cluster.connect();
                await this.fetchMetadata();
                this.metadata.setAssignment(this.metadata.getTopicPartitions());
                await this.fetchOffsets();
                await this.consumerGroup?.init();
                break;
            } catch (error) {
                log.error('Failed to start consumer', error);
                log.debug(`Restarting consumer in 1 second...`);
                await delay(1000);

                if (this.stopRequested) return this.stopRunning();
                await this.shutdown(false);
                if (this.stopRequested) return this.stopRunning();
            }
        }
        if (this.stopRequested) return this.stopRunning();

        this.startFetchManager();
    }

    private stopRunning() {
        this.running = false;

        const hooks = this.stopHooks;
        this.stopHooks = [];
        hooks.forEach((hook) => hook());
    }

    @trace()
    public async close(force = false): Promise<void> {
        this.closed = true;
        this.stopRequested = true;

        await this.shutdown(!force);
    }

    private async restart() {
        await this.shutdown(true);

        if (this.closed) return;

        await this.resume();
    }

    private async shutdown(drain: boolean) {
        if (drain && this.running) {
            const drained = new Promise<void>((resolve) => this.stopHooks.push(resolve));
            this.stopRequested = true;

            await this.fetchManager?.stop();
            await drained;
        }
        await this.consumerGroup
            ?.leaveGroup()
            .catch((error) => log.debug('Failed to leave group', { reason: (error as Error).message }));
        await this.cluster.disconnect().catch(() => {});
    }

    private async startFetchManager() {
        try {
            await this.runFetchManager();
        } catch (error) {
            log.error('Consumer stopped unexpectedly', error);
            void this.restart();
        } finally {
            this.stopRunning();
        }
    }

    private async runFetchManager() {
        const { groupId } = this.options;

        while (!this.stopRequested) {
            try {
                await this.consumerGroup?.join();
                if (this.stopRequested) break;

                // TODO: If leader is not available, find another read replica

                const topicPartitions = Object.entries(this.metadata.getAssignment()).flatMap(([topic, partitions]) =>
                    partitions.map((partition) => ({ topic, partition })),
                );
                const topicPartitionsByLeaderId = groupByLeaderId(
                    topicPartitions,
                    this.metadata.getTopicPartitionLeaderIds(),
                );
                const nodeAssignments = Object.entries(topicPartitionsByLeaderId).map(
                    ([leaderId, topicPartitions]) => ({
                        nodeId: parseInt(leaderId),
                        assignment: groupPartitionsByTopic(topicPartitions),
                    }),
                );

                this.fetchManager = new FetchManager({
                    fetch: this.fetch.bind(this),
                    process: this.process.bind(this),
                    nodeAssignments,
                });
                await this.fetchManager.start();

                if (!nodeAssignments.length) {
                    await this.waitForReassignment();
                }
            } catch (error) {
                await this.fetchManager?.stop();

                if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.REBALANCE_IN_PROGRESS) {
                    log.debug('Rebalance in progress...', { apiName: error.apiName, groupId });
                    continue;
                }
                if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.FENCED_INSTANCE_ID) {
                    log.debug('New consumer with the same groupInstanceId joined. Exiting the consumer...');
                    this.close();
                    break;
                }
                if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.NOT_COORDINATOR) {
                    log.debug('Not coordinator. Searching for new coordinator...');
                    await this.consumerGroup?.findCoordinator();
                    continue;
                }
                if (error instanceof ConnectionError) {
                    log.debug(`${error.message}. Restarting consumer...`, { stack: error.stack });
                    void this.restart();
                    break;
                }
                log.error((error as Error).message, error);

                log.debug(`Restarting consumer in 1 second...`);
                await delay(1000);

                void this.restart();
                break;
            }
        }
    }

    private async waitForReassignment() {
        const { groupId } = this.options;

        log.debug('No partitions assigned. Waiting for reassignment...', { groupId });
        while (!this.stopRequested) {
            await delay(1000);
            this.consumerGroup?.handleLastHeartbeat();
        }
    }

    @trace()
    private async process(response: FetchResponse) {
        const { options } = this;
        const { retrier } = options;

        this.consumerGroup?.handleLastHeartbeat();

        const topicPartitions: Record<string, Set<number>> = {};
        const messages = response.responses.flatMap((response) => {
            const topic =
                'topicName' in response ? response.topicName : this.metadata.getTopicNameById(response.topicId);
            topicPartitions[topic] ??= new Set();

            return response.partitions.flatMap(({ partitionIndex, records }) => {
                topicPartitions[topic].add(partitionIndex);
                return records.flatMap(({ baseTimestamp, baseOffset, records }) =>
                    records.flatMap((message): Required<Message> => ({
                        topic,
                        partition: partitionIndex,
                        key: message.key ?? null,
                        value: message.value ?? null,
                        headers: Object.fromEntries(message.headers.map(({ key, value }) => [key, value])),
                        timestamp: baseTimestamp + BigInt(message.timestampDelta),
                        offset: baseOffset + BigInt(message.offsetDelta),
                    })),
                );
            });
        });
        if (!messages.length) {
            return;
        }

        const commitOffset = async () => {
            await this.consumerGroup?.offsetCommit(topicPartitions);
            this.offsetManager.flush(topicPartitions);
        };

        const commitOffsetSafely = () =>
            commitOffset().catch((error) => log.debug('Failed to commit offsets', { reason: getErrorMessage(error) }));

        const resolveOffset = (message: Pick<Required<Message>, 'topic' | 'partition' | 'offset'>) =>
            this.offsetManager.resolve(message.topic, message.partition, message.offset + 1n);

        const abortController = new AbortController();
        const onRebalance = () => {
            abortController.abort();
            void commitOffsetSafely();
        };
        this.once('rebalanceInProgress', onRebalance);

        try {
            await retrier(() =>
                options.onBatch(
                    messages.filter((message) => !this.offsetManager.isResolved(message)),
                    { resolveOffset, abortSignal: abortController.signal },
                ),
            );
        } catch (error) {
            await commitOffsetSafely();
            throw error;
        } finally {
            this.off('rebalanceInProgress', onRebalance);
        }

        if (!abortController.signal.aborted) {
            response.responses.forEach((response) => {
                response.partitions.forEach(({ partitionIndex, records }) => {
                    records.forEach(({ baseOffset, lastOffsetDelta }) => {
                        const topic =
                            'topicName' in response
                                ? response.topicName
                                : this.metadata.getTopicNameById(response.topicId);
                        this.offsetManager.resolve(topic, partitionIndex, baseOffset + BigInt(lastOffsetDelta) + 1n);
                    });
                });
            });
        }

        await commitOffset();
    }

    private async fetch(nodeId: number, assignment: Assignment): Promise<FetchResponse> {
        return withRetry(this.handleError.bind(this))(async () => {
            const { rackId, maxWaitMs, minBytes, maxBytes, partitionMaxBytes, isolationLevel } = this.options;

            this.consumerGroup?.handleLastHeartbeat();

            return this.cluster.sendRequestToNode(nodeId)(API.FETCH, {
                maxWaitMs,
                minBytes,
                maxBytes,
                isolationLevel,
                sessionId: 0,
                sessionEpoch: -1,
                topics: Object.entries(assignment).map(([topicName, partitions]) => ({
                    topicId: this.metadata.getTopicIdByName(topicName),
                    topicName,
                    partitions: partitions.map((partition) => ({
                        partition,
                        currentLeaderEpoch: -1,
                        fetchOffset: this.offsetManager.getCurrentOffset(topicName, partition),
                        lastFetchedEpoch: -1,
                        logStartOffset: -1n,
                        partitionMaxBytes,
                    })),
                })),
                forgottenTopicsData: [],
                rackId,
            });
        });
    }

    private async fetchMetadata() {
        return withRetry(this.handleError.bind(this))(async () => {
            const { topics, allowTopicAutoCreation } = this.options;
            await this.metadata.fetchMetadata({ topics, allowTopicAutoCreation });
        });
    }

    private async fetchOffsets(): Promise<void> {
        return withRetry(this.handleError.bind(this))(async () => {
            const { fromTimestamp } = this.options;
            await this.offsetManager.fetchOffsets({ fromTimestamp });
        });
    }

    private async handleError(error: unknown) {
        await handleApiError(error).catch(async (error) => {
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.NOT_LEADER_OR_FOLLOWER) {
                log.debug('Refreshing metadata', { reason: error.message });
                await this.fetchMetadata();
                return;
            }
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.OFFSET_OUT_OF_RANGE) {
                log.warn('Offset out of range. Resetting offsets.');
                await this.fetchOffsets();
                return;
            }
            throw error;
        });
    }
}
