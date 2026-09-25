import { randomUUID } from 'crypto';
import { API, API_ERROR } from '../api';
import { ConsumerGroupHeartbeatResponse } from '../api/consumer-group-heartbeat';
import { Assignment } from '../api/sync-group';
import { delay } from '../utils/delay';
import { KafkaTSApiError } from '../utils/error';
import { withRetry } from '../utils/retry';
import { ConsumerGroup } from './consumer-group';

const JOIN_EPOCH = 0;
const LEAVE_EPOCH = -1;
const STATIC_LEAVE_EPOCH = -2;

const REJOIN_ERROR_CODES: number[] = [API_ERROR.FENCED_MEMBER_EPOCH, API_ERROR.UNKNOWN_MEMBER_ID];

const toKey = (assignment: Assignment) =>
    JSON.stringify(
        Object.entries(assignment)
            .filter(([, partitions]) => partitions.length)
            .map(([topic, partitions]) => [topic, partitions.toSorted((a, b) => a - b)])
            .sort(),
    );

export class ConsumerProtocolGroup extends ConsumerGroup {
    private heartbeatIntervalMs = 5000;
    private targetAssignment: Assignment | null = null;

    public override async init() {
        await this.findCoordinator();
        this.memberId = randomUUID();
        this.resetMembership();
    }

    public override async join() {
        if (this.generationId === JOIN_EPOCH) {
            this.targetAssignment = null;
            this.options.metadata.setAssignment({});
            while (!this.targetAssignment) {
                await withRetry(this.handleError.bind(this))(() => this.sendHeartbeat(this.generationId));
                if (!this.targetAssignment) await delay(this.heartbeatIntervalMs);
            }
        }
        this.options.metadata.setAssignment(this.targetAssignment!);
        await this.offsetFetch();
        this.startHeartbeater(this.heartbeatIntervalMs);
        await this.heartbeat();
    }

    public override async heartbeat() {
        try {
            await this.sendHeartbeat(this.generationId);
        } catch (error) {
            if (error instanceof KafkaTSApiError && REJOIN_ERROR_CODES.includes(error.errorCode)) {
                this.resetMembership();
            }
            throw error;
        }
        this.options.consumer.emit('heartbeat');

        if (toKey(this.targetAssignment!) !== toKey(this.options.metadata.getAssignment())) {
            throw new KafkaTSApiError(API_ERROR.REBALANCE_IN_PROGRESS, 'Assignment changed', null);
        }
    }

    public override async leaveGroup() {
        if (this.coordinatorId === -1) return;

        this.stopHeartbeater();
        await this.sendHeartbeat(this.options.groupInstanceId ? STATIC_LEAVE_EPOCH : LEAVE_EPOCH);
    }

    protected override resetMembership() {
        this.generationId = JOIN_EPOCH;
    }

    private async sendHeartbeat(memberEpoch: number) {
        const { cluster, groupId, groupInstanceId, rebalanceTimeoutMs, topics, metadata } = this.options;

        const response = await cluster.sendRequest(API.CONSUMER_GROUP_HEARTBEAT, {
            groupId,
            memberId: this.memberId,
            memberEpoch,
            instanceId: groupInstanceId,
            rackId: null,
            rebalanceTimeoutMs,
            subscribedTopicNames: topics,
            serverAssignor: null,
            topicPartitions:
                memberEpoch === JOIN_EPOCH
                    ? []
                    : Object.entries(metadata.getAssignment()).map(([topic, partitions]) => ({
                          topicId: metadata.getTopicIdByName(topic),
                          partitions,
                      })),
        });
        this.generationId = response.memberEpoch;
        this.heartbeatIntervalMs = response.heartbeatIntervalMs;
        if (response.assignment) {
            this.targetAssignment = this.toAssignment(response.assignment);
        }
    }

    private toAssignment({ topicPartitions }: NonNullable<ConsumerGroupHeartbeatResponse['assignment']>) {
        return Object.fromEntries(
            topicPartitions.map(({ topicId, partitions }) => [
                this.options.metadata.getTopicNameById(topicId),
                partitions,
            ]),
        );
    }
}
