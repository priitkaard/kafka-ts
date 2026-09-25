import { API, API_ERROR, handleApiError } from '../api';
import { KEY_TYPE } from '../api/find-coordinator';
import { Assignment, MemberAssignment } from '../api/sync-group';
import { Cluster } from '../cluster';
import { KafkaTSApiError, KafkaTSError } from '../utils/error';
import { log } from '../utils/logger';
import { withRetry } from '../utils/retry';
import { createTracer } from '../utils/tracer';
import { Consumer } from './consumer';
import { ConsumerMetadata } from './consumer-metadata';
import { OffsetManager } from './offset-manager';

const trace = createTracer('ConsumerGroup');

export type ConsumerGroupOptions = {
    cluster: Cluster;
    topics: string[];
    groupId: string;
    groupInstanceId: string | null;
    sessionTimeoutMs: number;
    rebalanceTimeoutMs: number;
    metadata: ConsumerMetadata;
    offsetManager: OffsetManager;
    consumer: Consumer;
};

export class ConsumerGroup {
    protected coordinatorId = -1;
    protected memberId = '';
    protected generationId = -1;
    private leaderId = '';
    private skipAssignment = false;
    private memberIds: string[] = [];
    private heartbeatInterval: NodeJS.Timeout | null = null;
    private heartbeatError: KafkaTSError | null = null;

    constructor(protected options: ConsumerGroupOptions) {}

    @trace()
    public async init() {
        await this.findCoordinator();
        this.memberId = '';
    }

    @trace()
    public async join() {
        await this.joinGroup();
        await this.syncGroup();
        await this.offsetFetch();
        this.startHeartbeater();
    }

    protected async startHeartbeater(intervalMs = this.options.sessionTimeoutMs / 10) {
        this.stopHeartbeater();
        this.heartbeatError = null;

        let isHeartbeating = false;
        const heartbeatInterval = setInterval(async () => {
            if (isHeartbeating) return;
            isHeartbeating = true;

            try {
                await this.heartbeat();
            } catch (error) {
                if (this.heartbeatInterval !== heartbeatInterval) return;

                this.heartbeatError = error as KafkaTSError;
                this.options.consumer.emit('heartbeatError', this.heartbeatError);
                if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.REBALANCE_IN_PROGRESS) {
                    this.options.consumer.emit('rebalanceInProgress');
                }
            } finally {
                isHeartbeating = false;
            }
        }, intervalMs);
        this.heartbeatInterval = heartbeatInterval;
    }

    protected async stopHeartbeater() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }
    }

    public handleLastHeartbeat() {
        if (this.heartbeatError) {
            throw this.heartbeatError;
        }
    }

    public async findCoordinator(): Promise<void> {
        return withRetry(this.handleError.bind(this))(async () => {
            const { coordinators } = await this.options.cluster.sendRequest(API.FIND_COORDINATOR, {
                keyType: KEY_TYPE.GROUP,
                keys: [this.options.groupId],
            });
            this.coordinatorId = coordinators[0].nodeId;

            await this.options.cluster.setSeedBroker(this.coordinatorId);
            this.heartbeatError = null;
        });
    }

    private async joinGroup(): Promise<void> {
        return withRetry(this.handleError.bind(this))(async () => {
            const { cluster, groupId, groupInstanceId, sessionTimeoutMs, rebalanceTimeoutMs, topics } = this.options;
            const response = await cluster.sendRequest(API.JOIN_GROUP, {
                groupId,
                groupInstanceId,
                memberId: this.memberId,
                sessionTimeoutMs,
                rebalanceTimeoutMs,
                protocolType: 'consumer',
                protocols: [{ name: 'RoundRobinAssigner', metadata: { version: 0, topics } }],
                reason: null,
            });
            this.memberId = response.memberId;
            this.generationId = response.generationId;
            this.leaderId = response.leader;
            this.skipAssignment = response.skipAssignment;
            this.memberIds = response.members
                .map(({ memberId, groupInstanceId }) => ({ memberId, sortKey: groupInstanceId ?? memberId }))
                .sort((a, b) => a.sortKey.localeCompare(b.sortKey))
                .map(({ memberId }) => memberId);
        });
    }

    private async syncGroup(): Promise<void> {
        return withRetry(this.handleError.bind(this))(async () => {
            const { cluster, metadata, groupId, groupInstanceId } = this.options;

            let assignments: MemberAssignment[] = [];
            if (this.memberId === this.leaderId && !this.skipAssignment) {
                const memberAssignments = Object.entries(metadata.getTopicPartitions())
                    .flatMap(([topic, partitions]) => partitions.map((partition) => ({ topic, partition })))
                    .reduce(
                        (acc, { topic, partition }, index) => {
                            const memberId = this.memberIds[index % this.memberIds.length];
                            acc[memberId] ??= {};
                            acc[memberId][topic] ??= [];
                            acc[memberId][topic].push(partition);
                            return acc;
                        },
                        {} as Record<string, Assignment>,
                    );
                assignments = Object.entries(memberAssignments).map(([memberId, assignment]) => ({
                    memberId,
                    assignment,
                }));
                log.debug('Assigned partitions to members', { assignments });
            }

            const response = await cluster.sendRequest(API.SYNC_GROUP, {
                groupId,
                groupInstanceId,
                memberId: this.memberId,
                generationId: this.generationId,
                protocolType: 'consumer',
                protocolName: 'RoundRobinAssigner',
                assignments,
            });
            metadata.setAssignment(response.assignment);
            log.debug('Updated member assignment', { assignment: response.assignment });
        });
    }

    protected async offsetFetch(): Promise<void> {
        return withRetry(this.handleError.bind(this))(async () => {
            const { cluster, groupId, topics, metadata, offsetManager } = this.options;

            const assignment = metadata.getAssignment();
            const request = {
                groups: [
                    {
                        groupId,
                        topics: topics
                            .map((topic) => ({
                                name: topic,
                                topicId: metadata.getTopicIdByName(topic),
                                partitionIndexes: assignment[topic] ?? [],
                            }))
                            .filter(({ partitionIndexes }) => partitionIndexes.length),
                    },
                ].filter(({ topics }) => topics.length),
                requireStable: true,
            };
            if (!request.groups.length) return;

            const response = await cluster.sendRequest(API.OFFSET_FETCH, request);

            const topicPartitions: Record<string, Set<number>> = {};
            response.groups.forEach((group) => {
                group.topics.forEach((topic) => {
                    const topicName = 'name' in topic ? topic.name : metadata.getTopicNameById(topic.topicId);
                    topicPartitions[topicName] ??= new Set();
                    topic.partitions.forEach(({ partitionIndex, committedOffset }) => {
                        if (committedOffset >= 0) {
                            topicPartitions[topicName].add(partitionIndex);
                            offsetManager.resolve(topicName, partitionIndex, committedOffset);
                        }
                    });
                });
            });
            offsetManager.flush(topicPartitions);
        });
    }

    public async offsetCommit(offsets: { topic: string; partition: number; offset: bigint }[]): Promise<void> {
        if (!offsets.length) return;

        return withRetry(this.handleError.bind(this))(async () => {
            const { cluster, groupId, groupInstanceId, metadata, consumer } = this.options;
            const topics = [...new Set(offsets.map(({ topic }) => topic))];
            await cluster.sendRequest(API.OFFSET_COMMIT, {
                groupId,
                groupInstanceId,
                memberId: this.memberId,
                generationIdOrMemberEpoch: this.generationId,
                topics: topics.map((topic) => ({
                    name: topic,
                    topicId: metadata.getTopicIdByName(topic),
                    partitions: offsets
                        .filter((offset) => offset.topic === topic)
                        .map(({ partition, offset }) => ({
                            partitionIndex: partition,
                            committedOffset: offset,
                            committedLeaderEpoch: -1,
                            committedMetadata: null,
                        })),
                })),
            });
            consumer.emit('offsetCommit');
        });
    }

    public async heartbeat() {
        const { cluster, groupId, groupInstanceId, consumer } = this.options;
        await cluster.sendRequest(API.HEARTBEAT, {
            groupId,
            groupInstanceId,
            memberId: this.memberId,
            generationId: this.generationId,
        });
        consumer.emit('heartbeat');
    }

    public async leaveGroup(): Promise<void> {
        return withRetry(this.handleError.bind(this))(async () => {
            if (this.coordinatorId === -1) {
                return;
            }

            const { cluster, groupId, groupInstanceId } = this.options;
            this.stopHeartbeater();
            if (groupInstanceId) {
                return;
            }

            await cluster.sendRequest(API.LEAVE_GROUP, {
                groupId,
                members: [{ memberId: this.memberId, groupInstanceId, reason: null }],
            });
        });
    }

    protected resetMembership() {
        this.memberId = '';
    }

    protected async handleError(error: unknown): Promise<void> {
        await handleApiError(error).catch(async (error) => {
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.NOT_COORDINATOR) {
                log.debug('Not coordinator. Searching for new coordinator...');
                await this.findCoordinator();
                return;
            }
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.MEMBER_ID_REQUIRED) {
                this.memberId = error.response.memberId;
                return;
            }
            if (error instanceof KafkaTSApiError && error.errorCode === API_ERROR.UNKNOWN_MEMBER_ID) {
                this.resetMembership();
            }
            throw error;
        });
    }
}
