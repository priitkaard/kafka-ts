import { FetchResponse } from '../api/fetch';
import { Assignment } from '../api/sync-group';
import { Metadata } from '../metadata';
import { Batch, Message } from '../types';
import { createTracer } from '../utils/tracer';
import { ConsumerGroup } from './consumer-group';
import { Fetcher } from './fetcher';
import { Processor } from './processor';

const trace = createTracer('FetchManager');

type FetchManagerOptions = {
    fetch: (nodeId: number, assignment: Assignment) => Promise<FetchResponse>;
    process: (batch: Batch) => Promise<void>;
    batchSize?: number | null;
    metadata: Metadata;
    consumerGroup?: ConsumerGroup;
    nodeAssignments: { nodeId: number; assignment: Assignment }[];
};

type Checkpoint = { kind: 'checkpoint'; fetcherId: number };
type Entry = Required<Message> | Checkpoint;

export class FetchManager {
    private queue: Entry[] = [];
    private isRunning = false;
    private fetchers: Fetcher[];
    private processor: Processor;
    private pollCallback: (() => void) | undefined;
    private fetcherCallbacks: Record<number, () => void> = {};

    constructor(private options: FetchManagerOptions) {
        const { fetch, process, nodeAssignments } = this.options;

        this.fetchers = nodeAssignments.map(
            ({ nodeId, assignment }, index) =>
                new Fetcher(index, {
                    nodeId,
                    assignment,
                    fetch,
                    onResponse: this.onResponse.bind(this),
                }),
        );
        this.processor = new Processor({ process, poll: this.poll.bind(this) });
    }

    @trace(() => ({ root: true }))
    public async start() {
        this.queue = [];
        this.isRunning = true;

        try {
            await Promise.all([...this.fetchers.map((fetcher) => fetcher.loop()), this.processor.loop()]);
        } finally {
            await this.stop();
        }
    }

    public async stop() {
        this.isRunning = false;

        const stopPromise = Promise.all([...this.fetchers.map((fetcher) => fetcher.stop()), this.processor.stop()]);

        this.pollCallback?.();

        Object.values(this.fetcherCallbacks).forEach((callback) => callback());
        this.fetcherCallbacks = {};

        await stopPromise;
    }

    @trace()
    public async poll(): Promise<Required<Message>[]> {
        if (!this.isRunning) {
            return [];
        }

        const { consumerGroup, batchSize } = this.options;
        consumerGroup?.handleLastHeartbeat();

        const batch = batchSize ? this.queue.splice(0, batchSize) : this.queue.splice(0);
        if (!batch.length) {
            await new Promise<void>((resolve) => (this.pollCallback = resolve));
            return this.poll();
        }

        const [checkpoints, messages] = partition(
            batch,
            (entry): entry is Checkpoint => 'kind' in entry && entry.kind === 'checkpoint',
        );

        checkpoints.forEach(({ fetcherId }) => this.fetcherCallbacks[fetcherId]?.());

        return messages;
    }

    @trace()
    private async onResponse(fetcherId: number, response: FetchResponse) {
        const { metadata, consumerGroup } = this.options;

        consumerGroup?.handleLastHeartbeat();

        const messages = response.responses.flatMap(({ topicId, partitions }) =>
            partitions.flatMap(({ partitionIndex, records }) =>
                records.flatMap(({ baseTimestamp, baseOffset, records }) =>
                    records.flatMap(
                        (message): Required<Message> => ({
                            topic: metadata.getTopicNameById(topicId),
                            partition: partitionIndex,
                            key: message.key ?? null,
                            value: message.value ?? null,
                            headers: Object.fromEntries(message.headers.map(({ key, value }) => [key, value])),
                            timestamp: baseTimestamp + BigInt(message.timestampDelta),
                            offset: baseOffset + BigInt(message.offsetDelta),
                        }),
                    ),
                ),
            ),
        );
        if (!messages.length) {
            return;
        }

        // wait until all broker batches have been processed or fetch manager is requested to stop
        await new Promise<void>((resolve) => {
            this.fetcherCallbacks[fetcherId] = resolve;
            this.queue.push(...messages, { kind: 'checkpoint', fetcherId });
            this.pollCallback?.();
        });

        consumerGroup?.handleLastHeartbeat();
    }
}

const partition = (batch: Entry[], predicate: (entry: Entry) => entry is Checkpoint) => {
    const checkpoints: Checkpoint[] = [];
    const messages: Required<Message>[] = [];

    for (const entry of batch) {
        if (predicate(entry)) {
            checkpoints.push(entry);
        } else {
            messages.push(entry);
        }
    }

    return [checkpoints, messages] as const;
};
