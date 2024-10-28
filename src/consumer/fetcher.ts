import { EventEmitter } from 'stream';
import { API } from '../api';
import { Assignment } from '../api/sync-group';
import { createTracer } from '../utils/tracer';
import { ConsumerGroup } from './consumer-group';

const trace = createTracer('Fetcher');

type FetcherOptions = {
    nodeId: number;
    assignment: Assignment;
    consumerGroup?: ConsumerGroup;
    fetch: (nodeId: number, assignment: Assignment) => Promise<Awaited<ReturnType<(typeof API.FETCH)['response']>>>;
    onResponse: (fetcherId: number, response: Awaited<ReturnType<(typeof API.FETCH)['response']>>) => Promise<void>;
};

export class Fetcher extends EventEmitter<{ stopped: [] }> {
    private isRunning = false;

    constructor(
        private fetcherId: number,
        private options: FetcherOptions,
    ) {
        super();
    }

    public async loop() {
        this.isRunning = true;

        try {
            while (this.isRunning) {
                await this.step();
            }
        } finally {
            this.isRunning = false;
            this.emit('stopped');
        }
    }

    @trace()
    private async step() {
        const { nodeId, assignment, consumerGroup, fetch, onResponse } = this.options;

        const response = await fetch(nodeId, assignment);
        if (!this.isRunning) {
            return;
        }
        consumerGroup?.handleLastHeartbeat();
        await onResponse(this.fetcherId, response);
        consumerGroup?.handleLastHeartbeat();
    }

    public async stop() {
        if (!this.isRunning) {
            return;
        }

        const stopPromise = new Promise<void>((resolve) => {
            this.once('stopped', resolve);
        });
        this.isRunning = false;
        return stopPromise;
    }
}
