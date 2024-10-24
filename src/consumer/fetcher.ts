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

export class Fetcher extends EventEmitter<{ stop: []; stopped: []; data: []; drain: [] }> {
    private isRunning = false;

    constructor(
        private fetcherId: number,
        private options: FetcherOptions,
    ) {
        super();
    }

    public async loop() {
        const { nodeId, assignment, consumerGroup, fetch, onResponse } = this.options;
        
        this.isRunning = true;
        this.once('stop', () => (this.isRunning = false));
        
        try {
            while (this.isRunning) {
                const response = await fetch(nodeId, assignment);
                await consumerGroup?.handleLastHeartbeat();
                await onResponse(this.fetcherId, response);
                await consumerGroup?.handleLastHeartbeat();
            }
        } finally {
            this.isRunning = false;
            this.emit('stopped');
        }
    }

    @trace()
    public async stop() {
        if (!this.isRunning) {
            return;
        }

        this.emit('stop');
        return new Promise<void>((resolve) => {
            this.once('stopped', resolve);
        });
    }
}
