import { API, API_ERROR } from "../api";
import { IsolationLevel } from "../api/fetch";
import { Assignment } from "../api/sync-group";
import { Cluster } from "../cluster";
import { distributeAssignmentsToNodes } from "../distributors/assignments-to-replicas";
import { Message } from "../types";
import { delay } from "../utils/delay";
import { ConnectionError, KafkaTSApiError } from "../utils/error";
import { defaultRetrier, Retrier } from "../utils/retrier";
import { ConsumerGroup } from "./consumer-group";
import { ConsumerMetadata } from "./consumer-metadata";
import { OffsetManager } from "./offset-manager";

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
    retrier?: Retrier;
} & ({ onMessage: (message: Message) => unknown } | { onBatch: (messages: Message[]) => unknown });

export class Consumer {
    private options: Required<ConsumerOptions>;
    private metadata: ConsumerMetadata;
    private consumerGroup: ConsumerGroup | undefined;
    private offsetManager: OffsetManager;
    private stopHook: (() => void) | undefined;

    constructor(
        private cluster: Cluster,
        options: ConsumerOptions,
    ) {
        this.options = {
            ...options,
            groupId: options.groupId ?? null,
            groupInstanceId: options.groupInstanceId ?? null,
            rackId: options.rackId ?? "",
            sessionTimeoutMs: options.sessionTimeoutMs ?? 30_000,
            rebalanceTimeoutMs: options.rebalanceTimeoutMs ?? 60_000,
            maxWaitMs: options.maxWaitMs ?? 5000,
            minBytes: options.minBytes ?? 1,
            maxBytes: options.maxBytes ?? 1_000_000,
            partitionMaxBytes: options.partitionMaxBytes ?? 1_000_000,
            isolationLevel: options.isolationLevel ?? IsolationLevel.READ_UNCOMMITTED,
            allowTopicAutoCreation: options.allowTopicAutoCreation ?? false,
            fromBeginning: options.fromBeginning ?? false,
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
              })
            : undefined;
    }

    public async start(): Promise<void> {
        const { topics, allowTopicAutoCreation, fromBeginning } = this.options;

        this.stopHook = undefined;

        try {
            await this.cluster.connect();
            await this.metadata.fetchMetadataIfNecessary({ topics, allowTopicAutoCreation });
            this.metadata.setAssignment(this.metadata.getTopicPartitions());
            await this.offsetManager.fetchOffsets({ fromBeginning });
            await this.consumerGroup?.join();
        } catch (error) {
            console.error(error);
            console.debug(`Restarting consumer in 1 second...`);
            await delay(1000);

            if (this.stopHook) return (this.stopHook as () => void)();
            return this.close(true).then(() => this.start());
        }
        this.fetchLoop();
    }

    private fetchLoop = async () => {
        const { options } = this;
        const { retrier } = options;

        let nodeAssignments: { nodeId: number; assignment: Assignment }[] = [];
        let shouldReassign = true;

        while (!this.stopHook) {
            if (shouldReassign || !nodeAssignments) {
                nodeAssignments = Object.entries(
                    distributeAssignmentsToNodes(
                        this.metadata.getAssignment(),
                        this.metadata.getTopicPartitionReplicaIds(),
                    ),
                ).map(([nodeId, assignment]) => ({ nodeId: parseInt(nodeId), assignment }));
                shouldReassign = false;
            }

            try {
                for (const { nodeId, assignment } of nodeAssignments) {
                    const batch = await this.fetch(nodeId, assignment);
                    const messages = batch.responses.flatMap(({ topicId, partitions }) =>
                        partitions.flatMap(({ partitionIndex, records }) =>
                            records.flatMap(({ baseTimestamp, baseOffset, records }) =>
                                records.map(
                                    (message): Required<Message> => ({
                                        topic: this.metadata.getTopicNameById(topicId),
                                        partition: partitionIndex,
                                        key: message.key ?? null,
                                        value: message.value ?? null,
                                        headers: Object.fromEntries(
                                            message.headers.map(({ key, value }) => [key, value]),
                                        ),
                                        timestamp: baseTimestamp + BigInt(message.timestampDelta),
                                        offset: baseOffset + BigInt(message.offsetDelta),
                                    }),
                                ),
                            ),
                        ),
                    );

                    if ("onBatch" in options) {
                        await retrier(() => options.onBatch(messages));

                        messages.forEach(({ topic, partition, offset }) =>
                            this.offsetManager.resolve(topic, partition, offset + 1n),
                        );
                    } else if ("onMessage" in options) {
                        for (const message of messages) {
                            await retrier(() => options.onMessage(message));

                            const { topic, partition, offset } = message;
                            this.offsetManager.resolve(topic, partition, offset + 1n);
                        }
                    }
                    await this.consumerGroup?.offsetCommit();
                    await this.consumerGroup?.handleLastHeartbeat();
                }

                if (!nodeAssignments.length) {
                    console.debug("No partitions assigned. Waiting for reassignment...");
                    await delay(this.options.maxWaitMs);
                    await this.consumerGroup?.handleLastHeartbeat();
                }
            } catch (error) {
                if ((error as KafkaTSApiError).errorCode === API_ERROR.REBALANCE_IN_PROGRESS) {
                    console.debug("Rebalance in progress...");
                    shouldReassign = true;
                    continue;
                }
                if ((error as KafkaTSApiError).errorCode === API_ERROR.FENCED_INSTANCE_ID) {
                    console.debug("New consumer with the same groupInstanceId joined. Exiting the consumer...");
                    this.close();
                    break;
                }
                if (
                    error instanceof ConnectionError ||
                    (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.NOT_COORDINATOR)
                ) {
                    console.debug(`${error.message}. Restarting consumer...`);
                    this.close().then(() => this.start());
                    break;
                }
                console.error(error);
                await this.consumerGroup?.offsetCommit();
                break;
            }
        }
        this.stopHook?.();
    };

    public async close(force = false): Promise<void> {
        if (!force) {
            await new Promise<void>((resolve) => {
                this.stopHook = resolve;
            });
        }
        await this.consumerGroup
            ?.leaveGroup()
            .catch((error) => console.warn(`Failed to leave group: ${error.message}`));
        await this.cluster.disconnect().catch((error) => console.warn(`Failed to disconnect: ${error.message}`));
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
