import { describe, expect, it, vi } from 'vitest';
import { API, API_ERROR } from '../api';
import { KafkaTSApiError } from '../utils/error';
import { ConsumerMetadata } from './consumer-metadata';
import { ConsumerProtocolGroup } from './consumer-protocol-group';

const heartbeatResponse = (memberEpoch: number, partitions: number[] | null) => ({
    memberEpoch,
    heartbeatIntervalMs: 5000,
    assignment: partitions && { topicPartitions: [{ topicId: 'topic-id', partitions }] },
});

const createGroup = (groupInstanceId: string | null = null) => {
    const heartbeats: any[] = [];
    const responses: (object | Error)[] = [];
    const cluster = {
        sendRequest: vi.fn(async (api: unknown, request: any) => {
            if (api === API.OFFSET_FETCH) return { groups: [] };
            heartbeats.push(request);
            const response = responses.shift()!;
            if (response instanceof Error) throw response;
            return response;
        }),
    };
    const metadata = new ConsumerMetadata({ cluster } as never);
    vi.spyOn(metadata, 'getTopicNameById').mockReturnValue('topic');
    vi.spyOn(metadata, 'getTopicIdByName').mockReturnValue('topic-id');
    const group = new ConsumerProtocolGroup({
        cluster,
        metadata,
        groupId: 'group',
        groupInstanceId,
        topics: ['topic'],
        rebalanceTimeoutMs: 60_000,
        offsetManager: { flush: () => {} },
        consumer: { emit: vi.fn() },
    } as never);
    (group as any).coordinatorId = 1;
    (group as any).memberId = 'member';
    (group as any).generationId = 0;
    return { group, metadata, heartbeats, responses };
};

describe('ConsumerProtocolGroup', () => {
    it('joins with epoch 0 and acknowledges its assignment', async () => {
        const { group, metadata, heartbeats, responses } = createGroup();
        responses.push(heartbeatResponse(1, [0, 1]), heartbeatResponse(1, null));

        await group.join();
        (group as any).stopHeartbeater();

        expect(metadata.getAssignment()).toEqual({ topic: [0, 1] });
        expect(heartbeats.map(({ memberEpoch, topicPartitions }) => ({ memberEpoch, topicPartitions }))).toEqual([
            { memberEpoch: 0, topicPartitions: [] },
            { memberEpoch: 1, topicPartitions: [{ topicId: 'topic-id', partitions: [0, 1] }] },
        ]);
    });

    it('reports a changed assignment as a rebalance', async () => {
        const { group, responses } = createGroup();
        responses.push(heartbeatResponse(1, [0, 1]), heartbeatResponse(1, null), heartbeatResponse(2, [0]));
        await group.join();
        (group as any).stopHeartbeater();

        await expect(group.heartbeat()).rejects.toThrow(/REBALANCE_IN_PROGRESS/);
    });

    it('rejoins with epoch 0 after being fenced', async () => {
        const { group, responses } = createGroup();
        responses.push(heartbeatResponse(1, [0]), heartbeatResponse(1, null));
        await group.join();
        (group as any).stopHeartbeater();

        responses.push(new KafkaTSApiError(API_ERROR.FENCED_MEMBER_EPOCH, null, {}));
        await expect(group.heartbeat()).rejects.toThrow(/FENCED_MEMBER_EPOCH/);

        expect((group as any).generationId).toBe(0);
    });

    it.each([
        [null, -1],
        ['instance', -2],
    ])('leaves with the right epoch for group instance id %s', async (groupInstanceId, memberEpoch) => {
        const { group, heartbeats, responses } = createGroup(groupInstanceId);
        responses.push(heartbeatResponse(memberEpoch, null));

        await group.leaveGroup();

        expect(heartbeats[0].memberEpoch).toBe(memberEpoch);
    });
});
