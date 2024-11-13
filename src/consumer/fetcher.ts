import { EventEmitter } from 'stream';
import { FetchResponse } from '../api/fetch';
import { Assignment } from '../api/sync-group';
import { createTracer } from '../utils/tracer';

const trace = createTracer('Fetcher');

type FetcherOptions = {
    nodeId: number;
    assignment: Assignment;
    fetch: (nodeId: number, assignment: Assignment) => Promise<FetchResponse>;
    process: (response: FetchResponse) => Promise<void>;
};

export class Fetcher extends EventEmitter<{ stopped: [] }> {
    private isRunning = false;

    constructor(private options: FetcherOptions) {
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
        const { nodeId, assignment, fetch, process } = this.options;

        const response = await fetch(nodeId, assignment);
        if (!this.isRunning) return;

        await process(response);
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
