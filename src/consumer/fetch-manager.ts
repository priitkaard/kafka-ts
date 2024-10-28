import { API } from '../api';
import { Assignment } from '../api/sync-group';
import { Metadata } from '../metadata';
import { Batch, Message } from '../types';
import { KafkaTSError } from '../utils/error';
import { createTracer } from '../utils/tracer';
import { ConsumerGroup } from './consumer-group';
import { Fetcher } from './fetcher';
import { Processor } from './processor';

const trace = createTracer('FetchManager');

export type BatchGranularity = 'partition' | 'topic' | 'broker';

type FetchManagerOptions = {
    fetch: (nodeId: number, assignment: Assignment) => Promise<Awaited<ReturnType<(typeof API.FETCH)['response']>>>;
    process: (batch: Batch) => Promise<void>;
    metadata: Metadata;
    consumerGroup?: ConsumerGroup;
    nodeAssignments: { nodeId: number; assignment: Assignment }[];
    batchGranularity: BatchGranularity;
    concurrency: number;
};

type Checkpoint = { kind: 'checkpoint'; fetcherId: number };
type Entry = Batch | Checkpoint;

export class FetchManager {
    private queue: Entry[] = [];
    private isRunning = false;
    private fetchers: Fetcher[];
    private processors: Processor[];
    private pollQueue: (() => void)[] = [];
    private fetcherCallbacks: Record<number, () => void> = {};

    constructor(private options: FetchManagerOptions) {
        const { fetch, process, consumerGroup, nodeAssignments, concurrency } = this.options;

        this.fetchers = nodeAssignments.map(
            ({ nodeId, assignment }, index) =>
                new Fetcher(index, {
                    nodeId,
                    assignment,
                    consumerGroup,
                    fetch,
                    onResponse: this.onResponse.bind(this),
                }),
        );
        this.processors = Array.from({ length: concurrency }).map(
            () => new Processor({ process, poll: this.poll.bind(this) }),
        );
    }

    @trace(() => ({ root: true }))
    public async start() {
        this.queue = [];
        this.isRunning = true;

        try {
            await Promise.all([
                ...this.fetchers.map((fetcher) => fetcher.loop()),
                ...this.processors.map((processor) => processor.loop()),
            ]);
        } finally {
            await this.stop();
        }
    }

    public async stop() {
        this.isRunning = false;

        const stopPromise = Promise.all([
            ...this.fetchers.map((fetcher) => fetcher.stop()),
            ...this.processors.map((processor) => processor.stop()),
        ]);

        this.pollQueue.forEach((resolve) => resolve());
        this.pollQueue = [];

        Object.values(this.fetcherCallbacks).forEach((callback) => callback());
        this.fetcherCallbacks = {};

        await stopPromise;
    }

    @trace()
    public async poll(): Promise<Batch> {
        if (!this.isRunning) {
            return [];
        }

        const batch = this.queue.shift();
        if (!batch) {
            // wait until new data is available or fetch manager is requested to stop
            await new Promise<void>((resolve) => {
                this.pollQueue.push(resolve);
            });
            return this.poll();
        }

        if ('kind' in batch && batch.kind === 'checkpoint') {
            this.fetcherCallbacks[batch.fetcherId]?.();
            return this.poll();
        }

        this.pollQueue?.shift()?.();

        return batch as Exclude<Entry, Checkpoint>;
    }

    @trace()
    private async onResponse(fetcherId: number, response: Awaited<ReturnType<(typeof API.FETCH)['response']>>) {
        const { metadata, batchGranularity } = this.options;

        const batches = fetchResponseToBatches(response, batchGranularity, metadata);
        if (!batches.length) {
            return;
        }

        // wait until all broker batches have been processed or fetch manager is requested to stop
        await new Promise<void>((resolve) => {
            this.fetcherCallbacks[fetcherId] = resolve;
            this.queue.push(...batches, { kind: 'checkpoint', fetcherId });
            this.pollQueue?.shift()?.();
        });
    }
}

const fetchResponseToBatches = (
    batch: Awaited<ReturnType<typeof API.FETCH.response>>,
    batchGranularity: BatchGranularity,
    metadata: Metadata,
): Batch[] => {
    const brokerTopics = batch.responses.map(({ topicId, partitions }) =>
        partitions.map(({ partitionIndex, records }) =>
            records.flatMap(({ baseTimestamp, baseOffset, records }) =>
                records.map(
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

    switch (batchGranularity) {
        case 'broker':
            const messages = brokerTopics.flatMap((topicPartition) =>
                topicPartition.flatMap((partitionMessages) => partitionMessages),
            );
            return messages.length ? [messages] : [];
        case 'topic':
            return brokerTopics
                .map((topicPartition) => topicPartition.flatMap((partitionMessages) => partitionMessages))
                .filter((messages) => messages.length);
        case 'partition':
            return brokerTopics
                .flatMap((topicPartition) => topicPartition.map((partitionMessages) => partitionMessages))
                .filter((messages) => messages.length);
        default:
            throw new KafkaTSError(`Unhandled batch granularity: ${batchGranularity}`);
    }
};
