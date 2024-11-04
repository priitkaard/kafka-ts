import EventEmitter from 'events';
import { API, API_ERROR } from '../api';
import { IsolationLevel } from '../api/fetch';
import { Assignment } from '../api/sync-group';
import { Cluster } from '../cluster';
import { distributeMessagesToTopicPartitionLeaders } from '../distributors/messages-to-topic-partition-leaders';
import { Message } from '../types';
import { delay } from '../utils/delay';
import { ConnectionError, KafkaTSApiError } from '../utils/error';
import { log } from '../utils/logger';
import { defaultRetrier, Retrier } from '../utils/retrier';
import { createTracer } from '../utils/tracer';
import { ConsumerGroup } from './consumer-group';
import { ConsumerMetadata } from './consumer-metadata';
import { BatchGranularity, FetchManager } from './fetch-manager';
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
    batchGranularity?: BatchGranularity;
    concurrency?: number;
    retrier?: Retrier;
} & ({ onBatch: (messages: Required<Message>[]) => unknown } | { onMessage: (message: Required<Message>) => unknown });

export class Consumer extends EventEmitter<{ offsetCommit: [], heartbeat: [] }> {
    private options: Required<ConsumerOptions>;
    private metadata: ConsumerMetadata;
    private consumerGroup: ConsumerGroup | undefined;
    private offsetManager: OffsetManager;
    private fetchManager?: FetchManager;
    private stopHook: (() => void) | undefined;

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
            isolationLevel: options.isolationLevel ?? IsolationLevel.READ_UNCOMMITTED,
            allowTopicAutoCreation: options.allowTopicAutoCreation ?? false,
            fromBeginning: options.fromBeginning ?? false,
            batchGranularity: options.batchGranularity ?? 'broker',
            concurrency: options.concurrency ?? 1,
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
    }

    @trace()
    public async start(): Promise<void> {
        const { topics, allowTopicAutoCreation, fromBeginning } = this.options;

        this.stopHook = undefined;

        try {
            await this.cluster.connect();
            await this.metadata.fetchMetadataIfNecessary({ topics, allowTopicAutoCreation });
            this.metadata.setAssignment(this.metadata.getTopicPartitions());
            await this.offsetManager.fetchOffsets({ fromBeginning });
            await this.consumerGroup?.init();
        } catch (error) {
            log.error('Failed to start consumer', error);
            log.debug(`Restarting consumer in 1 second...`);
            await delay(1000);

            if (this.stopHook) return (this.stopHook as () => void)();
            return this.close(true).then(() => this.start());
        }
        this.startFetchManager();
    }

    @trace()
    public async close(force = false): Promise<void> {
        if (!force) {
            await new Promise<void>(async (resolve) => {
                this.stopHook = resolve;
                await this.fetchManager?.stop();
            });
        }
        await this.consumerGroup?.leaveGroup().catch((error) => log.debug(`Failed to leave group: ${error.message}`));
        await this.cluster.disconnect().catch((error) => log.debug(`Failed to disconnect: ${error.message}`));
    }

    private async startFetchManager() {
        const { batchGranularity, concurrency } = this.options;

        while (!this.stopHook) {
            await this.consumerGroup?.join();

            // TODO: If leader is not available, find another read replica
            const nodeAssignments = Object.entries(
                distributeMessagesToTopicPartitionLeaders(
                    Object.entries(this.metadata.getAssignment()).flatMap(([topic, partitions]) =>
                        partitions.map((partition) => ({ topic, partition })),
                    ),
                    this.metadata.getTopicPartitionLeaderIds(),
                ),
            ).map(([nodeId, assignment]) => ({
                nodeId: parseInt(nodeId),
                assignment: Object.fromEntries(
                    Object.entries(assignment).map(([topic, partitions]) => [
                        topic,
                        Object.keys(partitions).map(Number),
                    ]),
                ),
            }));

            const numPartitions = Object.values(this.metadata.getAssignment()).flat().length;
            const numProcessors = Math.min(concurrency, numPartitions);

            this.fetchManager = new FetchManager({
                fetch: this.fetch.bind(this),
                process: this.process.bind(this),
                metadata: this.metadata,
                consumerGroup: this.consumerGroup,
                nodeAssignments,
                batchGranularity,
                concurrency: numProcessors,
            });

            try {
                await this.fetchManager.start();

                if (!nodeAssignments.length) {
                    log.debug('No partitions assigned. Waiting for reassignment...');
                    await delay(this.options.maxWaitMs);
                    this.consumerGroup?.handleLastHeartbeat();
                }
            } catch (error) {
                await this.fetchManager.stop();

                if ((error as KafkaTSApiError).errorCode === API_ERROR.REBALANCE_IN_PROGRESS) {
                    log.debug('Rebalance in progress...');
                    continue;
                }
                if ((error as KafkaTSApiError).errorCode === API_ERROR.FENCED_INSTANCE_ID) {
                    log.debug('New consumer with the same groupInstanceId joined. Exiting the consumer...');
                    this.close();
                    break;
                }
                if (
                    error instanceof ConnectionError ||
                    (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.NOT_COORDINATOR)
                ) {
                    log.debug(`${error.message}. Restarting consumer...`);
                    this.close().then(() => this.start());
                    break;
                }
                log.error((error as Error).message, error);

                log.debug(`Restarting consumer in 1 second...`);
                await delay(1000);

                this.close().then(() => this.start());
                break;
            }
        }
        this.stopHook?.();
    }

    @trace((messages) => ({ count: messages.length }))
    private async process(messages: Required<Message>[]) {
        const { options } = this;
        const { retrier } = options;

        const topicPartitions: Record<string, Set<number>> = {};
        for (const { topic, partition } of messages) {
            topicPartitions[topic] ??= new Set();
            topicPartitions[topic].add(partition);
        }

        const commit = async () => {
            await this.consumerGroup?.offsetCommit(topicPartitions);
            this.offsetManager.flush(topicPartitions);
        };

        if ('onBatch' in options) {
            await retrier(() => options.onBatch(messages));

            messages.forEach(({ topic, partition, offset }) =>
                this.offsetManager.resolve(topic, partition, offset + 1n),
            );
        } else if ('onMessage' in options) {
            for (const message of messages) {
                await retrier(() => options.onMessage(message)).catch(async (error) => {
                    await commit().catch();
                    throw error;
                });

                const { topic, partition, offset } = message;
                this.offsetManager.resolve(topic, partition, offset + 1n);
            }
        }

        await commit();
    }

    private fetch(nodeId: number, assignment: Assignment) {
        const { rackId, maxWaitMs, minBytes, maxBytes, partitionMaxBytes, isolationLevel } = this.options;

        return this.cluster.sendRequestToNode(nodeId)(API.FETCH, {
            maxWaitMs,
            minBytes,
            maxBytes,
            isolationLevel,
            sessionId: 0,
            sessionEpoch: -1,
            topics: Object.entries(assignment).map(([topic, partitions]) => ({
                topicId: this.metadata.getTopicIdByName(topic),
                partitions: partitions.map((partition) => ({
                    partition,
                    currentLeaderEpoch: -1,
                    fetchOffset: this.offsetManager.getCurrentOffset(topic, partition),
                    lastFetchedEpoch: -1,
                    logStartOffset: 0n,
                    partitionMaxBytes,
                })),
            })),
            forgottenTopicsData: [],
            rackId,
        });
    }
}
