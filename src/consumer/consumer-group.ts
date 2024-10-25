import { API, API_ERROR } from '../api';
import { KEY_TYPE } from '../api/find-coordinator';
import { Assignment, MemberAssignment } from '../api/sync-group';
import { Cluster } from '../cluster';
import { KafkaTSApiError, KafkaTSError } from '../utils/error';
import { createTracer } from '../utils/tracer';
import { ConsumerMetadata } from './consumer-metadata';
import { OffsetManager } from './offset-manager';

const trace = createTracer('ConsumerGroup');

type ConsumerGroupOptions = {
    cluster: Cluster;
    topics: string[];
    groupId: string;
    groupInstanceId: string | null;
    sessionTimeoutMs: number;
    rebalanceTimeoutMs: number;
    metadata: ConsumerMetadata;
    offsetManager: OffsetManager;
};

export class ConsumerGroup {
    private coordinatorId = -1;
    private memberId = '';
    private generationId = -1;
    private leaderId = '';
    private memberIds: string[] = [];
    private heartbeatInterval: NodeJS.Timeout | null = null;
    private heartbeatError: KafkaTSError | null = null;

    constructor(private options: ConsumerGroupOptions) {}

    @trace()
    public async join() {
        await this.findCoordinator();
        await this.options.cluster.setSeedBroker(this.coordinatorId);

        await this.joinGroup();
        await this.syncGroup();
        await this.offsetFetch();
        this.startHeartbeater();
    }

    private async startHeartbeater() {
        this.heartbeatInterval = setInterval(async () => {
            try {
                await this.heartbeat();
            } catch (error) {
                this.heartbeatError = error as KafkaTSError;
            }
        }, 5000);
    }

    private async stopHeartbeater() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
        }
    }

    public async handleLastHeartbeat() {
        if (this.heartbeatError) {
            throw this.heartbeatError;
        }
    }

    private async findCoordinator() {
        const { coordinators } = await this.options.cluster.sendRequest(API.FIND_COORDINATOR, {
            keyType: KEY_TYPE.GROUP,
            keys: [this.options.groupId],
        });
        this.coordinatorId = coordinators[0].nodeId;
    }

    private async joinGroup(): Promise<void> {
        const { cluster, groupId, groupInstanceId, sessionTimeoutMs, rebalanceTimeoutMs, topics } = this.options;
        try {
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
            this.memberIds = response.members.map((member) => member.memberId);
        } catch (error) {
            if ((error as KafkaTSApiError).errorCode === API_ERROR.MEMBER_ID_REQUIRED) {
                this.memberId = (error as KafkaTSApiError).response.memberId;
                return this.joinGroup();
            }
            throw error;
        }
    }

    private async syncGroup() {
        const { cluster, metadata, groupId, groupInstanceId } = this.options;

        let assignments: MemberAssignment[] = [];
        if (this.memberId === this.leaderId) {
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
                    {} as Record<string, Record<string, number[]>>,
                );
            assignments = Object.entries(memberAssignments).map(([memberId, assignment]) => ({ memberId, assignment }));
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
        metadata.setAssignment(JSON.parse(response.assignments || '{}') as Assignment);
    }

    private async offsetFetch() {
        const { cluster, groupId, topics, metadata, offsetManager } = this.options;

        const assignment = metadata.getAssignment();
        const request = {
            groups: [
                {
                    groupId,
                    memberId: this.memberId,
                    memberEpoch: -1,
                    topics: topics
                        .map((topic) => ({ name: topic, partitionIndexes: assignment[topic] ?? [] }))
                        .filter(({ partitionIndexes }) => partitionIndexes.length),
                },
            ].filter(({ topics }) => topics.length),
            requireStable: true,
        };
        if (!request.groups.length) return;

        const response = await cluster.sendRequest(API.OFFSET_FETCH, request);
        response.groups.forEach((group) => {
            group.topics.forEach((topic) => {
                topic.partitions
                    .filter(({ committedOffset }) => committedOffset >= 0)
                    .forEach(({ partitionIndex, committedOffset }) =>
                        offsetManager.resolve(topic.name, partitionIndex, committedOffset),
                    );
            });
        });
        offsetManager.flush();
    }

    public async offsetCommit() {
        const { cluster, groupId, groupInstanceId, offsetManager } = this.options;
        const request = {
            groupId,
            groupInstanceId,
            memberId: this.memberId,
            generationIdOrMemberEpoch: this.generationId,
            topics: Object.entries(offsetManager.pendingOffsets).map(([topic, partitions]) => ({
                name: topic,
                partitions: Object.entries(partitions).map(([partition, offset]) => ({
                    partitionIndex: parseInt(partition),
                    committedOffset: offset,
                    committedLeaderEpoch: -1,
                    committedMetadata: null,
                })),
            })),
        };
        if (!request.topics.length) {
            return;
        }
        await cluster.sendRequest(API.OFFSET_COMMIT, request);
        offsetManager.flush();
    }

    public async heartbeat() {
        const { cluster, groupId, groupInstanceId } = this.options;
        await cluster.sendRequest(API.HEARTBEAT, {
            groupId,
            groupInstanceId,
            memberId: this.memberId,
            generationId: this.generationId,
        });
    }

    public async leaveGroup() {
        const { cluster, groupId, groupInstanceId } = this.options;
        this.stopHeartbeater();
        try {
            await cluster.sendRequest(API.LEAVE_GROUP, {
                groupId,
                members: [{ memberId: this.memberId, groupInstanceId, reason: null }],
            });
        } catch (error) {
            if ((error as KafkaTSApiError).errorCode === API_ERROR.FENCED_INSTANCE_ID) {
                return;
            }
            throw error;
        }
    }
}
