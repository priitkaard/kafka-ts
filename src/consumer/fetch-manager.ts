import EventEmitter from 'events';
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

export class FetchManager extends EventEmitter<{ data: []; checkpoint: [number]; stop: [] }> {
    private queue: Entry[] = [];
    private isRunning = false;
    private fetchers: Fetcher[];
    private processors: Processor[];

    constructor(private options: FetchManagerOptions) {
        super();

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

    public async start() {
        this.queue = [];
        this.isRunning = true;

        try {
            await Promise.all([
                ...this.fetchers.map((fetcher) => fetcher.loop()),
                ...this.processors.map((processor) => processor.loop()),
            ]);
        } finally {
            this.isRunning = false;
            this.emit('stop');
        }
    }

    @trace()
    public async stop() {
        this.isRunning = false;
        this.emit('stop');

        await Promise.all([
            ...this.fetchers.map((fetcher) => fetcher.stop()),
            ...this.processors.map((processor) => processor.stop()),
        ]);
    }

    public async poll(): Promise<Batch> {
        if (!this.isRunning) {
            return [];
        }

        const batch = this.queue.shift();
        if (!batch) {
            await new Promise<void>((resolve) => {
                const onData = () => {
                    this.removeListener('stop', onStop);
                    resolve();
                };
                const onStop = () => {
                    this.removeListener('data', onData);
                    resolve();
                };
                this.once('data', onData);
                this.once('stop', onStop);
            });
            return this.poll();
        }

        if ('kind' in batch && batch.kind === 'checkpoint') {
            this.emit('checkpoint', batch.fetcherId);
            return this.poll();
        }

        return batch as Exclude<Entry, Checkpoint>;
    }

    private async onResponse(fetcherId: number, response: Awaited<ReturnType<(typeof API.FETCH)['response']>>) {
        const { metadata, batchGranularity } = this.options;

        const batches = fetchResponseToBatches(response, batchGranularity, metadata);
        if (batches.length) {
            this.queue.push(...batches);
            this.queue.push({ kind: 'checkpoint', fetcherId });

            this.emit('data');
            await new Promise<void>((resolve) => {
                const onCheckpoint = (id: number) => {
                    if (id === fetcherId) {
                        this.removeListener('stop', onStop);
                        resolve();
                    }
                };
                const onStop = () => {
                    this.removeListener('checkpoint', onCheckpoint);
                    resolve();
                };
                this.once('checkpoint', onCheckpoint);
                this.once('stop', onStop);
            });
        }
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
            return brokerTopics.flatMap((topicPartition) =>
                topicPartition.map((partitionMessages) => partitionMessages),
            );
        default:
            throw new KafkaTSError(`Unhandled batch granularity: ${batchGranularity}`);
    }
};
