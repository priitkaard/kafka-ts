import { FetchResponse } from '../api/fetch';
import { Assignment } from '../api/sync-group';
import { createTracer } from '../utils/tracer';
import { Fetcher } from './fetcher';

const trace = createTracer('FetchManager');

type FetchManagerOptions = {
    fetch: (nodeId: number, assignment: Assignment) => Promise<FetchResponse>;
    process: (response: FetchResponse) => Promise<void>;
    nodeAssignments: { nodeId: number; assignment: Assignment }[];
};

export class FetchManager {
    private fetchers: Fetcher[];

    constructor(private options: FetchManagerOptions) {
        const { fetch, process, nodeAssignments } = this.options;

        this.fetchers = nodeAssignments.map(
            ({ nodeId, assignment }) => new Fetcher({ nodeId, assignment, fetch, process }),
        );
    }

    @trace(() => ({ root: true }))
    public async start() {
        try {
            await Promise.all(this.fetchers.map((fetcher) => fetcher.loop()));
        } finally {
            await this.stop();
        }
    }

    public async stop() {
        return Promise.all(this.fetchers.map((fetcher) => fetcher.stop()));
    }
}
